"""Proxy requests using AWS Lambda"""
import base64
import boto3
import json
import time

from threading import Semaphore
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

from shared.proxy import proxy_single_request


def short_lived_handler(event, context):
    """Handle a single request and return it immediately"""
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']

    print method, url
    for header, value in requestHeaders.iteritems():
        print '  %s: %s' % (header, value)

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

MIN_MILLIS_REMAINING = 30 * 1000
MESSAGE_ATTRIBUTE_NAMES = ['All']
MAX_NUM_SQS_MESSAGES = 10
MAX_NUM_THREADS = 10
MAX_QUEUED_REQUESTS = 25
MAX_IDLE_POLLS = 1

assert MAX_QUEUED_REQUESTS > MAX_NUM_SQS_MESSAGES, \
    'The maximum number of messages to fetch in one poll ' \
    'cannot be less than the max number of queued requests'

pool = None
sqs = None
def _lazy_worker_init():
    """Build the thread pool lazily"""
    if pool is None:
        global pool, sqs
        pool = ThreadPoolExecutor(MAX_NUM_THREADS)
        sqs = boto3.client('sqs')


def process_single_message(message, responseQueue, queuedRequestsSemaphore):
    try:
        taskId = message['MessageAttributes']['taskId']['StringValue']
        requestParams = json.loads(message['Body'])

        method = requestParams['method']
        url = requestParams['url']
        requestHeaders = requestParams['headers']
        requestBody = message['MessageAttributes']['body']['BinaryValue']

        print method, url
        for header, value in requestHeaders.iteritems():
            print '  %s: %s' % (header, value)

        response = proxy_single_request(method, url, requestHeaders,
                                        requestBody, gzipResult=True)

        responseBody = {
            'statusCode': response.statusCode,
            'headers': response.headers
        }
        messageAttributes = {
            'body': {
                'BinaryValue': response.content,
                'DataType': 'binary'
            },
            'taskId': {
                'StringValue': taskId,
                'DataType': 'string'
            }
        }
        responseQueue.send_message(MessageBody=json.dumps(responseBody),
                                   MessageAttributes=messageAttributes)
    finally:
        queuedRequestsSemaphore.release()


def long_lived_handler(event, context):
    """"Handle multiple requests using SQS as a task queue"""
    startTime = time.time()
    _lazy_worker_init()

    workerId = int(event['workerId'])
    requestQueueName = event['requestQueue']
    responseQueueName = event['responseQueue']

    print 'Running long-lived as %s' % workerId
    print 'Consuming requests from: %s' % requestQueueName
    print 'Returning response to: %s' % responseQueueName

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

        queuedRequestsSemaphore.acquire(MAX_NUM_SQS_MESSAGES)
        messages = requestQueue.receive_messages(
            MessageAttributeNames=MESSAGE_ATTRIBUTE_NAMES,
            MaxNumberOfMessages=MAX_NUM_SQS_MESSAGES)

        # Release extra semaphores
        for _ in xrange(MAX_NUM_SQS_MESSAGES - len(messages)):
            queuedRequestsSemaphore.release()

        if len(messages) > 0:
            # Delete the messages asynchronously
            for message in messages:
                pool.submit(process_single_message, message, responseQueue)
                numRequestsProxied += 1

            requestQueue.delete_messages(
                Entries=[{
                    'Id': message['MessageId'],
                    'ReceiptHandle': message['ReceiptHandle']
                } for message in messages])

            idlePolls = 0
        else:
            idlePolls += 1

        if idlePolls > MAX_IDLE_POLLS:
            exitReason = 'Idle timeout reached'
            break

    # Wait for any straggling requests
    for _ in xrange(MAX_QUEUED_REQUESTS):
        queuedRequestsSemaphore.acquire()

    return {
        'workerId': workerId,
        'workerLifetime': time.time() - startTime,
        'numRequestsProxied': numRequestsProxied,
        'exitReason': exitReason
    }


def handler(event, context):
    if 'longLived' in event and event['longLived'] == True:
        return long_lived_handler(event, context)
    else:
        return short_lived_handler(event, context)

