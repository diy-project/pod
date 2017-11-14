import base64
import boto3
import json
import logging

from random import SystemRandom
from threading import Semaphore

from ..proxy import AbstractRequestProxy, ProxyResponse
from ..workers import LambdaSqsTaskConfig, WorkerManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    def __init__(self, functions, maxParallelRequests=5):
        self.functions = functions

        self.secureRandom = SystemRandom()
        self.lambdaClient = boto3.client('lambda')
        self.lambdaRateSema = Semaphore(maxParallelRequests)

    def request(self, method, url, headers, body):
        logger.info('Proxying %s %s with Lamdba', method, url)
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if body is not None:
            args['body64'] = base64.b64encode(body)

        self.lambdaRateSema.acquire()
        try:
            response = self.lambdaClient.invoke(
                FunctionName=self.secureRandom.choice(self.functions),
                Payload=json.dumps(args))
        finally:
            self.lambdaRateSema.release()

        if response['StatusCode'] != 200:
            logger.error('%s: status=%d', response['FunctionError'],
                         response['StatusCode'])
            return ProxyResponse(status_code=500, headers={}, content='')

        payload = json.loads(response['Payload'].read())
        if 'content64' in payload:
            content = base64.b64decode(payload['content64'])
        else:
            content = ''
        return ProxyResponse(status_code=payload['statusCode'],
                             headers=payload['headers'],
                             content=content)

class LongLivedLambdaProxy(AbstractRequestProxy):
    """Return a function that queues requests in SQS"""

    def __init__(self, functions, maxLambdas=5, verbose=False):
        self.verbose = verbose

        class ProxyTask(LambdaSqsTaskConfig):

            secureRandom = SystemRandom()

            @property
            def queue_prefix(self):
                return 'lambda-proxy'

            @property
            def lambda_function(self):
                return self.secureRandom.choice(functions)

            @property
            def max_workers(self):
                return maxLambdas

            @property
            def load_factor(self):
                return 5

            _result_attributes = ['body']
            @property
            def result_attributes(self):
                return self._result_attributes

            def pre_invoke_callback(self, workerId, workerArgs):
                logger.info('Starting worker: %d', workerId)
                workerArgs['longLived'] = True

            def post_return_callback(self, workerId, workerResponse):
                logger.info('Worker %d ran for %ds and proxied %d requests',
                            workerId, workerResponse['workerLifetime'],
                            workerResponse['numRequestsProxied'])

        self.workerManager = WorkerManager(ProxyTask())

    def request(self, method, url, headers, body):
        messageAttributes = {
            'body': {
                'BinaryValue': body if body is not None else b'',
                'DataType': 'binary'
            }
        }
        messageBody = json.dumps({
            'method': method,
            'url': url,
            'headers': headers,
            'hasBody': body is None
        })
        response = self.workerManager.execute(messageBody, messageAttributes,
                                              timeout=10)
        if response is None:
            return ProxyResponse(status_code=500, headers={}, content='')


