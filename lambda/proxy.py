"""Proxy requests using AWS Lambda"""
import boto3
import hashlib
import json
import os
import time
import traceback

from base64 import b64encode, b64decode
from concurrent.futures import ThreadPoolExecutor
from threading import Semaphore

from shared.proxy import proxy_single_request
from shared.workers import LambdaSqsResult, LambdaSqsTask

DEBUG = os.environ.get('VERBOSE', False)


def log_request(method, url, headers):
    print method, url
    for header, value in headers.iteritems():
        print '  %s: %s' % (header, value)


# The body can be up to 6MB, this leaves a bit of lee-way
MAX_LAMBDA_BODY_PAYLOAD_SIZE = int(5.8 * 1024 * 1024)


def short_lived_handler(event, context):
    """Handle a single request and return it immediately"""
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']
    s3BucketName = event.get('s3Bucket', None)

    if DEBUG: log_request(method, url, requestHeaders)

    if 'body64' in event:
        requestBody = b64decode(event['body64'])
    else:
        requestBody = None

    response = proxy_single_request(method, url, requestHeaders,
                                    requestBody, gzipResult=True)
    ret = {
        'statusCode': response.statusCode,
        'headers': response.headers
    }

    if response.content:
        content64 = b64encode(response.content)
        if not s3BucketName or len(content64) < MAX_LAMBDA_BODY_PAYLOAD_SIZE:
            ret['content64'] = content64
        else:
            md5 = hashlib.md5()
            md5.update(response.content)
            key = md5.hexdigest()
            s3Bucket = boto3.resource('s3').Bucket(s3BucketName)
            s3Bucket.put_object(Key=key, Body=response.content,
                                StorageClass='REDUCED_REDUNDANCY')
            ret['s3Key'] = key

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


def send_response_as_fragments(task, response, responseQueue,
                               encodedMessageBody):
    contentLen = len(response.content)
    numFragments = contentLen / MAX_PAYLOAD_PER_SQS_MESSAGE + 1
    if (contentLen % MAX_PAYLOAD_PER_SQS_MESSAGE + len(encodedMessageBody) >
        MAX_PAYLOAD_PER_SQS_MESSAGE):
        # Cannot pack headers into last message
        numFragments += 1
    if numFragments > MAX_NUM_FRAGMENTS:
        raise Exception('Too many fragments: %d', numFragments)

    if DEBUG: print 'Sending response in %d chunks' % numFragments
    for i in xrange(numFragments):
        part = LambdaSqsResult(taskId=task.taskId,
                               numFragments=numFragments,
                               fragmentId=i)
        if i * MAX_PAYLOAD_PER_SQS_MESSAGE < contentLen:
            baseIdx = i * MAX_PAYLOAD_PER_SQS_MESSAGE
            part.add_binary_attribute(
                'data',
                response.content[baseIdx:baseIdx+MAX_PAYLOAD_PER_SQS_MESSAGE])

        if i == numFragments - 1:
            part.set_body(encodedMessageBody)
        else:
            part.set_body(' ')
        responseQueue.send_message(MessageBody=part.body,
                                   MessageAttributes=part.messageAttributes)


def send_response_via_s3(task, response, responseQueue, s3Bucket,
                         encodedMessageBody):
    md5 = hashlib.md5()
    md5.update(response.content)
    key = md5.hexdigest()

    s3Bucket.put_object(Key=key, Body=response.content,
                        StorageClass='REDUCED_REDUNDANCY')

    result = LambdaSqsResult(taskId=task.taskId)
    result.add_string_attribute('s3', key)
    result.set_body(encodedMessageBody)
    responseQueue.send_message(MessageBody=result.body,
                               MessageAttributes=result.messageAttributes)


def send_response_directly(task, response, responseQueue,
                           encodedMessageBody):
    result = LambdaSqsResult(taskId=task.taskId)
    if response.content:
        result.add_binary_attribute('data', response.content)
    result.set_body(encodedMessageBody)
    responseQueue.send_message(MessageBody=result.body,
                               MessageAttributes=result.messageAttributes)


def send_response_to_message(task, response, responseQueue, s3Bucket):
    messageBody = {
        'statusCode': response.statusCode,
        'headers': response.headers,
    }
    encodedMessageBody = b64encode(json.dumps(messageBody).encode('zlib'))
    estimatedLength = len(encodedMessageBody) + len(response.content)
    if estimatedLength <= MAX_PAYLOAD_PER_SQS_MESSAGE:
        send_response_directly(task, response, responseQueue, encodedMessageBody)
    else:
        if s3Bucket:
            send_response_via_s3(task, response, responseQueue, s3Bucket,
                                 encodedMessageBody)
        else:
            send_response_as_fragments(task, response, responseQueue,
                                       encodedMessageBody)


def process_single_message(message, responseQueue, s3Bucket, queuedRequestsSemaphore):
    """Proxy a single message in the thread pool"""
    try:
        task = LambdaSqsTask.from_message(message)
        requestParams = json.loads(task.body)

        method = requestParams['method']
        url = requestParams['url']
        requestHeaders = requestParams['headers']

        if DEBUG: log_request(method, url, requestHeaders)

        requestBody = None
        if task.has_attribute('data'):
            requestBody = task.get_binary_attribute('data')

        response = proxy_single_request(method, url, requestHeaders,
                                        requestBody, gzipResult=True)
        send_response_to_message(task, response, responseQueue, s3Bucket)
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
    s3BucketName = event.get('s3Bucket', None)

    if DEBUG:
        print 'Running long-lived as: worker', workerId
        print 'Consuming requests from:', requestQueueName
        print 'Returning responses to:', responseQueueName

    s3Bucket = None
    if s3BucketName is not None:
        s3Bucket = boto3.resource('s3').Bucket(s3BucketName)
        if DEBUG:
            print 'Serving large responses from s3:', s3BucketName

    requestQueue = sqs.get_queue_by_name(QueueName=requestQueueName)
    responseQueue = sqs.get_queue_by_name(QueueName=responseQueueName)

    numRequestsProxied = 0
    queuedRequestsSemaphore = Semaphore(MAX_QUEUED_REQUESTS)

    idlePolls = 0
    while True:
        millisRemaining = context.get_remaining_time_in_millis()
        if millisRemaining < MIN_MILLIS_REMAINING:
            exitReason = 'Remaining time low: %d' % millisRemaining
            break

        # Acquire worst case number of semaphores
        for _ in xrange(MAX_NUM_SQS_MESSAGES):
            queuedRequestsSemaphore.acquire()

        if DEBUG: print 'Polling SQS for new requests'
        messages = requestQueue.receive_messages(
            MessageAttributeNames=MESSAGE_ATTRIBUTE_NAMES,
            MaxNumberOfMessages=MAX_NUM_SQS_MESSAGES)

        # Release extra semaphores
        for _ in xrange(MAX_NUM_SQS_MESSAGES - len(messages)):
            queuedRequestsSemaphore.release()

        if len(messages) > 0:
            if DEBUG: print 'Handling %d proxy requests' % len(messages)
            for message in messages:
                pool.submit(process_single_message, message,
                            responseQueue, s3Bucket,
                            queuedRequestsSemaphore)
                numRequestsProxied += 1

            requestQueue.delete_messages(
                Entries=[{
                    'Id': message.message_id,
                    'ReceiptHandle': message.receipt_handle
                } for message in messages])

            idlePolls = 0
        else:
            if DEBUG: print 'No new requests from queue'
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
