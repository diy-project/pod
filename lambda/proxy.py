import base64
import boto3
import requests
import time
from multiprocessing.pool import ThreadPool


AUTO_DECODED_CONTENTS = {'gzip', 'deflate'}


def proxy_request(method, url, headers, body):
    """Proxy a single request and return the response"""
    kwargs = {
        'headers': headers,
        'allow_redirects': False,
    }
    if body:
        kwargs['data'] = body

    with requests.request(method, url, **kwargs) as response:
        statusCode = response.status_code
        responseHeaders = {k: response.headers[k] for k in response.headers}
        responseBody = response.content

        if 'Transfer-Encoding' in responseHeaders and \
            responseHeaders['Transfer-Encoding'] == 'chunked':
            del responseHeaders['Transfer-Encoding']
            responseHeaders['Content-Length'] = len(responseBody)

        if 'Content-Encoding' in responseHeaders and \
            responseHeaders['Content-Encoding'] in AUTO_DECODED_CONTENTS:
            del responseHeaders['Content-Encoding']
            responseHeaders['Content-Length'] = len(responseBody)

        ret = {
            'statusCode': statusCode,
            'headers': responseHeaders
        }
        if responseBody:
            ret['content64'] = base64.b64encode(responseBody)
        return ret


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

    return proxy_request(method, url, requestHeaders, requestBody)


MAX_EXECUTORS = 20
EXECUTOR_POOL = None # builds the thread pool lazily


def long_lived_handler(event, context):
    """"Handle multiple requests using SQS as a task queue"""
    startTime = time.time()

    workerId = int(event['workerId'])
    requestQueueName = event['requestQueue']
    responseQueueName = event['responseQueue']

    sqs = boto3.resource('sqs')
    requestQueue = sqs.get_queue_by_name(QueueName=requestQueueName)
    responseQueue = sqs.get_queue_by_name(QueueName=responseQueueName)

    exitReason = ''
    numRequestsProxied = 0

    if EXECUTOR_POOL is None:
        global EXECUTOR_POOL
        EXECUTOR_POOL = ThreadPool(MAX_EXECUTORS)



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

