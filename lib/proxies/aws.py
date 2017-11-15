import boto3
import json
import logging
import time

from base64 import b64encode, b64decode
from concurrent.futures import ThreadPoolExecutor
from random import SystemRandom
from threading import Semaphore
from urlparse import urlparse

from lib.proxy import AbstractRequestProxy, ProxyResponse
from lib.workers import LambdaSqsTaskConfig, LambdaSqsTask, WorkerManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

_secureRandom = SystemRandom()


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    __lambda = boto3.client('lambda')

    def __init__(self, functions, maxParallelRequests):
        self.__functions = functions
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)

    def request(self, method, url, headers, body):
        logger.debug('Proxying %s %s with Lamdba', method, url)
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if body is not None:
            args['body64'] = b64encode(body)

        self.__lambdaRateSemaphore.acquire()
        try:
            response = self.__lambda.invoke(
                FunctionName=_secureRandom.choice(self.__functions),
                Payload=json.dumps(args))
        finally:
            self.__lambdaRateSemaphore.release()

        if response['StatusCode'] != 200:
            logger.error('%s: status=%d', response['FunctionError'],
                         response['StatusCode'])
            return ProxyResponse(statusCode=500, headers={}, content='')

        # TODO: this step sometimes fails
        payload = json.loads(response['Payload'].read())
        statusCode = payload['statusCode']
        headers = payload['headers']
        if 'content64' in payload:
            content = b64decode(payload['content64'])
        else:
            content = b''
        return ProxyResponse(statusCode=statusCode,
                             headers=headers,
                             content=content)

class LongLivedLambdaProxy(AbstractRequestProxy):
    """Return a function that queues requests in SQS"""

    def __init__(self, functions, maxLambdas, s3Bucket, verbose):
        self.__verbose = verbose
        self.__s3Bucket = s3Bucket
        if s3Bucket is not None:
            self.__s3 = boto3.client('s3')
            self.__s3DeletePool = ThreadPoolExecutor(1)

        class ProxyTask(LambdaSqsTaskConfig):

            @property
            def queue_prefix(self):
                return 'lambda-proxy'

            @property
            def lambda_function(self):
                return _secureRandom.choice(functions)

            @property
            def max_workers(self):
                return maxLambdas

            @property
            def load_factor(self):
                return 4

            def pre_invoke_callback(self, workerId, workerArgs):
                logger.debug('Starting worker: %d', workerId)
                workerArgs['longLived'] = True
                if s3Bucket:
                    workerArgs['s3Bucket'] = s3Bucket

            def post_return_callback(self, workerId, workerResponse):
                if workerResponse is not None:
                    logger.debug('Worker %d ran for %dms and proxied %d '
                                 'requests: Exit reason: %s',
                                 workerId,
                                 workerResponse['workerLifetime'],
                                 workerResponse['numRequestsProxied'],
                                 workerResponse['exitReason'])

        self.workerManager = WorkerManager(ProxyTask())

    def __delete_object_from_s3(self, key):
        self.__s3.delete_object(Bucket=self.__s3Bucket, Key=key)

    def __load_object_from_s3(self, key):
        result = self.__s3.get_object(Bucket=self.__s3Bucket, Key=key)
        ret = result['Body'].read()
        self.__s3DeletePool.submit(self.__delete_object_from_s3, key)
        return ret

    def request(self, method, url, headers, data):
        task = LambdaSqsTask()
        if data:
            task.add_binary_attribute('data', data)
        task.set_body(json.dumps({
            'method': method,
            'url': url,
            'headers': headers
        }))
        result = self.workerManager.execute(task, timeout=10)
        if result is None:
            return ProxyResponse(statusCode=500, headers={}, content='')

        if type(result) is list:
            # Fragmented response
            payload = {}
            dataChunks = []
            for part in result:
                if part.has_attribute('data'):
                    dataChunks.append(part.get_binary_attribute('data'))
                if len(part.body) > 1:
                    # We use a hack to send practically empty bodies
                    payload.update(json.loads(b64decode(part.body).decode('zlib')))
            content = b''.join(dataChunks)
        else:
            # Single message
            payload = json.loads(b64decode(result.body).decode('zlib'))
            if result.has_attribute('s3'):
                key = result.get_string_attribute('s3')
                content = self.__load_object_from_s3(key)
            elif result.has_attribute('data'):
                content = result.get_binary_attribute('data')
            else:
                content = b''
        statusCode = payload['statusCode']
        responseHeaders = payload['headers']
        return ProxyResponse(statusCode=statusCode, headers=responseHeaders,
                             content=content)

class HybridLambdaProxy(LongLivedLambdaProxy):

    def __init__(self, functions, *args):
        super(HybridLambdaProxy, self).__init__(functions, *args)
        self.__shortLivedProxy = ShortLivedLambdaProxy(functions, 5)
        self.__lastRequestTime = 0

    def request(self, method, url, headers, body):
        if self.should_use_short_lived_proxy(method, url, headers, body):
            logger.debug('Using short proxy for: %s %s', method, url[:50])
            return self.__shortLivedProxy.request(method, url, headers, body)
        else:
            logger.debug('Using long proxy for: %s %s', method, url[:50])
            return super(HybridLambdaProxy, self).request(
                method, url, headers, body)

    SHORT_LIVED_TYPES = ['.html', '.js', '.css', '.png', '.jpg', '.json']

    def should_use_short_lived_proxy(self, method, url, headers, body):
        curTime = time.time()
        try:
            if curTime - self.__lastRequestTime > 0.5: return True
        finally:
            self.__lastRequestTime = curTime

        # TODO: maybe use header info for cross-origin
        if method.upper() != 'GET':
            return False
        parsedUrl = urlparse(url)
        if len(parsedUrl.query) > 10:
            return False
        if len(parsedUrl.path.split('/')) < 3:
            return True
        if any(x in parsedUrl.path for x in HybridLambdaProxy.SHORT_LIVED_TYPES):
            return True
        return False
