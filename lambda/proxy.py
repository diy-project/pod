"""Proxy requests using AWS Lambda"""
import base64
import boto3
import json
import os
import time
import traceback

from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

from shared.proxy import proxy_single_request


DEBUG = os.environ.get('VERBOSE', False)


def log_request(method, url, headers):
    print method, url
    for header, value in headers.iteritems():
        print '  %s: %s' % (header, value)


def short_lived_handler(event, context):
    """Handle a single request and return it immediately"""
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']

    if DEBUG:
        log_request(method, url, requestHeaders)

    if 'body64' in event:
        requestBody = base64.b64decode(event['body64'])
    else:
        requestBody = None

    response = proxy_single_request(method, url, requestHeaders,
                                    requestBody, gzipResult=True)
    ret = {
        'statusCode': response.statusCode,
        'headers': response.headers
    }
    if response.content:
        ret['content64'] = base64.b64encode(response.content)
    return ret


# Long-lived handler constants

MESSAGE_ATTRIBUTE_NAMES = ['All']
MAX_NUM_SQS_MESSAGES = 5
MIN_MILLIS_REMAINING = int(os.environ.get('MIN_MILLIS_REMAINING', 10 * 1000))
MAX_QUEUED_REQUESTS = int(os.environ.get('MAX_QUEUED_REQUESTS', 10))
MAX_IDLE_POLLS = int(os.environ.get('MAX_IDLE_POLLS', 1))
MAX_NUM_FRAGMENTS = int(os.environ.get('MAX_NUM_FRAGMENTS', 20))
MAX_NUM_THREADS = min(MAX_QUEUED_REQUESTS, 10)

# This leaves 4KB
MAX_PAYLOAD_PER_SQS_MESSAGE = 252 * 1024


assert MAX_QUEUED_REQUESTS >= MAX_NUM_SQS_MESSAGES, \
    'The maximum number of messages to fetch in one poll ' \
    'cannot be less than the max number of queued requests'

pool = None
sqs = None
def _lazy_worker_init():
    """Build the thread pool lazily"""
    global pool, sqs
    if pool is None or sqs is None:
        pool = ThreadPoolExecutor(MAX_NUM_THREADS)
        sqs = boto3.resource('sqs')


def send_response_as_fragments(response, taskId, responseQueue,
                               messageBodyLen):
    contentLen = len(response.content)
    numFragments = contentLen / MAX_PAYLOAD_PER_SQS_MESSAGE + 1
    if (contentLen % MAX_PAYLOAD_PER_SQS_MESSAGE + messageBodyLen >
        MAX_PAYLOAD_PER_SQS_MESSAGE):
        # Cannot pack headers into last message
        numFragments += 1
    if numFragments > MAX_NUM_FRAGMENTS:
        raise Exception('Too many fragments: %d', numFragments)

    if DEBUG:
        print 'Sending response in %d chunks' % numFragments
    for i in xrange(numFragments):
        messageBody = {}
        if i == numFragments - 1:
            messageBody = {
                'statusCode': response.statusCode,
                'headers': response.headers,
                'hasBody': True
            }
        messageAttributes = {
            'taskId': {
                'StringValue': taskId,
                'DataType': 'String'
            },
            'numFragments': {
                'StringValue': str(numFragments),
                'DataType': 'Number'
            },
            'fragmentId': {
                'StringValue': str(i),
                'DataType': 'Number'
            },
        }
        if i * MAX_PAYLOAD_PER_SQS_MESSAGE < contentLen:
            baseIdx = i * MAX_PAYLOAD_PER_SQS_MESSAGE
            messageAttributes['data'] = {
                'BinaryValue': response.content[baseIdx:baseIdx+MAX_PAYLOAD_PER_SQS_MESSAGE],
                'DataType': 'Binary'
            }
        responseQueue.send_message(MessageBody=json.dumps(messageBody),
                                   MessageAttributes=messageAttributes)


def process_single_message(message, responseQueue, queuedRequestsSemaphore):
    """Proxy a single message in the thread pool"""
    try:
        taskId = message.message_id
        requestParams = json.loads(message.body)

        method = requestParams['method']
        url = requestParams['url']
        requestHeaders = requestParams['headers']
        hasBody = requestParams['hasBody']

        if DEBUG:
            log_request(method, url, requestHeaders)

        if hasBody:
            requestBody = message.message_attributes['body']['BinaryValue']
        else:
            requestBody = None

        response = proxy_single_request(method, url, requestHeaders,
                                        requestBody, gzipResult=True)

        hasBody = response.content is not None and len(response.content) > 0
        responseBody = {
            'statusCode': response.statusCode,
            'headers': response.headers,
            'hasBody': hasBody
        }
        messageAttributes = {
            'taskId': {
                'StringValue': taskId,
                'DataType': 'String'
            }
        }
        messageBody = json.dumps(responseBody)
        estimatedLength = len(messageBody) + len(response.content)
        if estimatedLength <= MAX_PAYLOAD_PER_SQS_MESSAGE:
            if hasBody:
                messageAttributes['body'] = {
                    'BinaryValue': response.content,
                    'DataType': 'Binary'
                }
            responseQueue.send_message(MessageBody=messageBody,
                                       MessageAttributes=messageAttributes)
        else:
            send_response_as_fragments(response, taskId, responseQueue,
                                       len(messageBody))
    except Exception, e:
        print traceback.format_exc(e)
    finally:
        queuedRequestsSemaphore.release()


def long_lived_handler(event, context):
    """"Handle multiple requests using SQS as a task queue"""
    startTime = time.time()
    _lazy_worker_init()

    workerId = int(event['workerId'])
    requestQueueName = event['taskQueue']
    responseQueueName = event['resultQueue']

    if DEBUG:
        print 'Running long-lived as: worker', workerId
        print 'Consuming requests from:', requestQueueName
        print 'Returning responses to:', responseQueueName

    requestQueue = sqs.get_queue_by_name(QueueName=requestQueueName)
    responseQueue = sqs.get_queue_by_name(QueueName=responseQueueName)

    numRequestsProxied = 0
    queuedRequestsSemaphore = Semaphore(MAX_QUEUED_REQUESTS)

    idlePolls = 0
    while True:
        if context.get_remaining_time_in_millis() < MIN_MILLIS_REMAINING:
            exitReason = 'Remaining time low'
            break

        # Acquire worst case number of semaphores
        for _ in xrange(MAX_NUM_SQS_MESSAGES):
            queuedRequestsSemaphore.acquire()

        if DEBUG:
            print 'Polling SQS for new requests'
        messages = requestQueue.receive_messages(
            MessageAttributeNames=MESSAGE_ATTRIBUTE_NAMES,
            MaxNumberOfMessages=MAX_NUM_SQS_MESSAGES)

        # Release extra semaphores
        for _ in xrange(MAX_NUM_SQS_MESSAGES - len(messages)):
            queuedRequestsSemaphore.release()

        if len(messages) > 0:
            if DEBUG:
                print 'Handling %d proxy requests' % len(messages)
            for message in messages:
                pool.submit(process_single_message, message,
                            responseQueue,
                            queuedRequestsSemaphore)
                numRequestsProxied += 1

            requestQueue.delete_messages(
                Entries=[{
                    'Id': message.message_id,
                    'ReceiptHandle': message.receipt_handle
                } for message in messages])

            idlePolls = 0
        else:
            if DEBUG:
                print 'No new requests from queue'
            idlePolls += 1

        if idlePolls > MAX_IDLE_POLLS:
            exitReason = 'Idle timeout reached'
            break

    # Wait for any straggling requests
    for _ in xrange(MAX_QUEUED_REQUESTS):
        queuedRequestsSemaphore.acquire()

    return {
        'workerId': workerId,
        'workerLifetime': int((time.time() - startTime) * 1000),
        'numRequestsProxied': numRequestsProxied,
        'exitReason': exitReason
    }


def handler(event, context):
    if 'longLived' in event and event['longLived'] == True:
        return long_lived_handler(event, context)
    else:
        return short_lived_handler(event, context)


def main(queueId):
    """Basic local testing"""
    event = {
        'longLived': True,
        'workerId': 0,
        'taskQueue': 'lambda-proxy_task_%d' % queueId,
        'resultQueue': 'lambda-proxy_result_%d' % queueId
    }
    class DummyContext(object):
        def get_remaining_time_in_millis(self):
            return MIN_MILLIS_REMAINING + 1
    context = DummyContext()
    while True:
        print handler(event, context)

if __name__ == '__main__':
    import sys
    DEBUG = True
    if len(sys.argv) < 2:
        print 'Usage: python proxy.py queueId'
        sys.exit(1)
    main(int(sys.argv[1]))
