import base64
import boto3
import json
import logging

from random import SystemRandom
from threading import Semaphore

from lib.proxy import AbstractRequestProxy, ProxyResponse
from lib.workers import LambdaSqsTaskConfig, WorkerManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    __secureRandom = SystemRandom()
    __lambda = boto3.client('lambda')

    def __init__(self, functions, maxParallelRequests=5):
        self.__secureRandom = SystemRandom()
        self.__functions = functions
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)

    def request(self, method, url, headers, body):
        logger.info('Proxying %s %s with Lamdba', method, url)
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if body is not None:
            args['body64'] = base64.b64encode(body)

        self.__lambdaRateSemaphore.acquire()
        try:
            response = self.__lambda.invoke(
                FunctionName=self.__secureRandom.choice(self.__functions),
                Payload=json.dumps(args))
        finally:
            self.__lambdaRateSemaphore.release()

        if response['StatusCode'] != 200:
            logger.error('%s: status=%d', response['FunctionError'],
                         response['StatusCode'])
            return ProxyResponse(statusCode=500, headers={}, content='')

        payload = json.loads(response['Payload'].read())
        if 'content64' in payload:
            content = base64.b64decode(payload['content64'])
        else:
            content = ''
        return ProxyResponse(statusCode=payload['statusCode'],
                             headers=payload['headers'],
                             content=content)

class LongLivedLambdaProxy(AbstractRequestProxy):
    """Return a function that queues requests in SQS"""

    def __init__(self, functions, maxLambdas=5, verbose=False):
        self.__verbose = verbose

        class ProxyTask(LambdaSqsTaskConfig):

            __secureRandom = SystemRandom()

            @property
            def queue_prefix(self):
                return 'lambda-proxy'

            @property
            def lambda_function(self):
                return self.__secureRandom.choice(functions)

            @property
            def max_workers(self):
                return maxLambdas

            @property
            def load_factor(self):
                return 5

            def pre_invoke_callback(self, workerId, workerArgs):
                logger.info('Starting worker: %d', workerId)
                workerArgs['longLived'] = True

            def post_return_callback(self, workerId, workerResponse):
                if workerResponse is not None:
                    logger.info('Worker %d ran for %dms and proxied %d '
                                'requests: Exit reason: %s',
                                workerId, workerResponse['workerLifetime'],
                                workerResponse['numRequestsProxied'],
                                workerResponse['exitReason'])

        self.workerManager = WorkerManager(ProxyTask())

    def request(self, method, url, headers, body):
        messageAttributes = {}
        hasBody = body is not None and len(body) > 0
        if hasBody:
            messageAttributes['body'] = {
                'BinaryValue': body if body is not None else b'',
                'DataType': 'Binary'
            }
        messageBody = json.dumps({
            'method': method,
            'url': url,
            'headers': headers,
            'hasBody': hasBody
        })
        result = self.workerManager.execute(messageBody, messageAttributes,
                                            timeout=10)
        if result is None:
            return ProxyResponse(statusCode=500, headers={}, content='')

        if type(result) is list:
            # Fragmented response
            payload = {}
            dataChunks = []
            for message in result:
                if 'data' in message.message_attributes:
                    dataChunks.append(message.message_attributes['data']['BinaryValue'])
                    payload.update(json.loads(message.body))
            content = b''.join(dataChunks)
        else:
            # Single message
            payload = json.loads(result.body)
            if payload['hasBody']:
                content = result.message_attributes['body']['BinaryValue']
            else:
                content = b''
        statusCode = payload['statusCode']
        responseHeaders = payload['headers']
        return ProxyResponse(statusCode=statusCode, headers=responseHeaders,
                             content=content)

class HybridLambdaProxy(LongLivedLambdaProxy):

    def __init__(self, functions, *args):
        super(HybridLambdaProxy, self).__init__(functions, *args)
        self.__fastProxy = ShortLivedLambdaProxy(functions)

    def request(self, method, url, headers, body):
        if len(url) < 50:
            return self.__fastProxy.request(method, url, headers, body)
        else:
            return super(HybridLambdaProxy, self).request(
                method, url, headers, body)
