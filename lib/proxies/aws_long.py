import boto3
import json
import logging

from base64 import b64decode
from random import SystemRandom

from concurrent.futures import ThreadPoolExecutor
from lib.proxy import AbstractRequestProxy, ProxyResponse
from lib.stats import LambdaStatsModel, S3StatsModel
from lib.workers import LambdaSqsTaskConfig, LambdaSqsTask, WorkerManager

logger = logging.getLogger(__name__)

random = SystemRandom()


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
                return random.choice(functions)

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
                    decodedBody = b64decode(part.body).decode('zlib')
                    payload.update(json.loads(decodedBody))
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