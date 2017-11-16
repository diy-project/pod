import boto3
import hashlib
import json
import logging
import time
from base64 import b64encode, b64decode
from random import SystemRandom
from threading import Semaphore
from urlparse import urlparse

from concurrent.futures import ThreadPoolExecutor
from lib.proxy import AbstractRequestProxy, ProxyResponse
from lib.stats import LambdaStatsModel, S3StatsModel
from lib.workers import LambdaSqsTaskConfig, LambdaSqsTask, WorkerManager

logger = logging.getLogger(__name__)

_secureRandom = SystemRandom()


def _get_region_from_arn(arn):
    elements = arn.split(':')
    return elements[3]


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    __lambda = boto3.client('lambda')

    MAX_BODY_LEN = int(5.8 * 1024 * 1024)

    def __init__(self, functions, maxParallelRequests, s3Bucket, stats):
        self.__functions = functions
        self.__functionToClient = {}
        self.__regionToClient = {}
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)
        self.__s3Bucket = s3Bucket

        if 'lambda' not in stats.models:
            stats.register_model('lambda', LambdaStatsModel())
        self.__lambdaStats = stats.get_model('lambda')

        if s3Bucket is not None:
            if 's3' not in stats.models:
                stats.register_model('s3', S3StatsModel())
            self.__s3Stats = stats.get_model('s3')
            self.__s3 = boto3.client('s3')
            self.__s3DeletePool = ThreadPoolExecutor(1)

    def __get_lambda_client(self, function):
        """Get a lambda client from the right region"""
        client = self.__functionToClient.get(function)
        if client is not None:
            return client
        if 'arn:' not in function:
            # using function name in the default region
            client = self.__lambda
            self.__functionToClient[function] = client
        else:
            region = _get_region_from_arn(function)
            client = self.__regionToClient.get(region)
            if client is None:
                client = boto3.client('lambda', region_name=region)
                self.__regionToClient[region] = client
            self.__functionToClient[function] = client
        return client

    def __delete_object_from_s3(self, key):
        self.__s3.delete_object(Bucket=self.__s3Bucket, Key=key)

    def __load_object_from_s3(self, key):
        result = self.__s3.get_object(Bucket=self.__s3Bucket, Key=key)
        ret = result['Body'].read()
        self.__s3DeletePool.submit(self.__delete_object_from_s3, key)
        self.__s3Stats.record_get(len(ret))
        return ret

    def __put_object_into_s3(self, data):
        md5 = hashlib.md5()
        md5.update(data)
        key = md5.hexdigest()
        s3Bucket = boto3.resource('s3').Bucket(self.__s3Bucket)
        s3Bucket.put_object(Key=key, Body=data,
                            StorageClass='REDUCED_REDUNDANCY')
        self.__s3Stats.record_get(len(data))
        return key

    def request(self, method, url, headers, body):
        logger.debug('Proxying %s %s with Lamdba', method, url)
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        requestS3Key = None
        try:
            if body is not None:
                encodedBody = b64encode(body)
                if len(encodedBody) <= ShortLivedLambdaProxy.MAX_BODY_LEN:
                    args['body64'] = encodedBody
                elif self.__s3Bucket is not None:
                    requestS3Key = self.__put_object_into_s3(encodedBody)
                    args['s3Key'] = requestS3Key
            if self.__s3Bucket is not None:
                args['s3Bucket'] = self.__s3Bucket

            function = _secureRandom.choice(self.__functions)
            lambdaClient = self.__get_lambda_client(function)

            self.__lambdaRateSemaphore.acquire()
            try:
                with self.__lambdaStats.record() as billingObject:
                    response = lambdaClient.invoke(
                        FunctionName=function,
                        Payload=json.dumps(args),
                        LogType='Tail')
                    billingObject.parse_log(response['LogResult'])
            finally:
                self.__lambdaRateSemaphore.release()
        finally:
            if requestS3Key is not None:
                self.__s3DeletePool.submit(self.__delete_object_from_s3,
                                           requestS3Key)

        if response['StatusCode'] != 200:
            logger.error('%s: status=%d', response['FunctionError'],
                         response['StatusCode'])
            return ProxyResponse(statusCode=500, headers={}, content='')

        if 'FunctionError' in response:
            logger.error('%s error: %s', response['FunctionError'],
                         response['Payload'].read())
            return ProxyResponse(statusCode=500, headers={}, content='')

        payload = json.loads(response['Payload'].read())
        statusCode = payload['statusCode']
        headers = payload['headers']
        if 'content64' in payload:
            content = b64decode(payload['content64'])
        elif 's3Key' in payload:
            content = self.__load_object_from_s3(payload['s3Key'])
        else:
            content = b''
        return ProxyResponse(statusCode=statusCode, headers=headers,
                             content=content)


class LongLivedLambdaProxy(AbstractRequestProxy):
    """Return a function that queues requests in SQS"""

    def __init__(self, functions, maxLambdas, s3Bucket, stats, verbose):

        # Supporting this across regions is not a priority since that would
        # incur costs for SQS and S3, and be error prone.
        if len(functions) > 1 and 'arn:' in functions[0]:
            raise NotImplementedError(
                'Only a single function may be specified by name for a '
                'long lived proxy')

        self.__verbose = verbose
        self.__s3Bucket = s3Bucket

        if 'lambda' not in stats.models:
            stats.register_model('lambda', LambdaStatsModel())
        self.__lambdaStats = stats.get_model('lambda')

        if s3Bucket is not None:
            if 's3' not in stats.models:
                stats.register_model('s3', S3StatsModel())
            self.__s3Stats = stats.get_model('s3')
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
                logger.info('Starting worker: %d', workerId)
                workerArgs['longLived'] = True
                if s3Bucket:
                    workerArgs['s3Bucket'] = s3Bucket

            def post_return_callback(self, workerId, workerResponse):
                if workerResponse is not None:
                    logger.info('Worker %d ran for %dms and proxied %d '
                                 'requests: Exit reason: %s',
                                 workerId,
                                 workerResponse['workerLifetime'],
                                 workerResponse['numRequestsProxied'],
                                 workerResponse['exitReason'])

        self.workerManager = WorkerManager(ProxyTask(), stats)

    def __delete_object_from_s3(self, key):
        self.__s3.delete_object(Bucket=self.__s3Bucket, Key=key)

    def __load_object_from_s3(self, key):
        result = self.__s3.get_object(Bucket=self.__s3Bucket, Key=key)
        ret = result['Body'].read()
        self.__s3DeletePool.submit(self.__delete_object_from_s3, key)
        self.__s3Stats.record_get(len(ret))
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

    def __init__(self, functions, maxLambdas, s3Bucket, stats, verbose):
        super(HybridLambdaProxy, self).__init__(functions, maxLambdas,
                                                s3Bucket, stats, verbose)
        self.__shortLivedProxy = ShortLivedLambdaProxy(functions, maxLambdas,
                                                       s3Bucket, stats)
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
