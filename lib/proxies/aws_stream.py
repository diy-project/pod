import boto3
import json
import logging
from random import SystemRandom
from threading import Semaphore

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from lib.proxy import AbstractStreamProxy
from lib.stats import LambdaStatsModel

logger = logging.getLogger(__name__)

random = SystemRandom()


def _get_region_from_arn(arn):
    elements = arn.split(':')
    return elements[3]


class StreamLambdaProxy(AbstractStreamProxy):
    """Invoke a lambda for each connection"""

    class Connection(AbstractStreamProxy.Connection):

        def __init__(self, host, port):
            self.host = host
            self.port = port

        def close(self):
            pass

        def __str__(self):
            return self.host + ':' + self.port

    def __init__(self, functions, maxParallelRequests,
                 pubKeyFile, streamServer, stats, maxIdleTimeout=5):
        self.__connIdleTimeout = maxIdleTimeout
        self.__functions = functions
        self.__functionToClient = {}
        self.__regionToClient = {}
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)
        self.__lambda = boto3.client('lambda')

        if 'lambda' not in stats.models:
            stats.register_model('lambda', LambdaStatsModel())
        self.__lambdaStats = stats.get_model('lambda')

        self.__streamServer = streamServer

        # Enable encryption
        self.__enableEncryption = False
        if pubKeyFile is not None:
            with open(pubKeyFile, 'rb') as ifs:
                self.__rsaCipher = PKCS1_OAEP.new(RSA.importKey(ifs.read()))
                self.__enableEncryption = True

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

    def connect(self, host, port):
        return StreamLambdaProxy.Connection(host, port)

    def stream(self, cliSock, servInfo):
        assert isinstance(servInfo, StreamLambdaProxy.Connection)
        socketId = '%016x' % random.getrandbits(128)
        invokeArgs = {
            'stream': True,
            'socketId': socketId,
            'streamServer': self.__streamServer.publicHostAndPort,
            'host': servInfo.host,
            'port': int(servInfo.port),
            'idleTimeout': self.__connIdleTimeout
        }

        function = random.choice(self.__functions)
        lambdaClient = self.__get_lambda_client(function)

        self.__lambdaRateSemaphore.acquire()
        try:
            self.__streamServer.take_ownership_of_socket(socketId, cliSock,
                                                         self.__connIdleTimeout)
            with self.__lambdaStats.record() as billingObject:
                invokeResponse = lambdaClient.invoke(
                    FunctionName=function,
                    Payload=json.dumps(invokeArgs),
                    LogType='Tail')
                billingObject.parse_log(invokeResponse['LogResult'])
        finally:
            self.__lambdaRateSemaphore.release()

        if invokeResponse['StatusCode'] != 200:
            logger.error('%s: status=%d', invokeResponse['FunctionError'],
                         invokeResponse['StatusCode'])
        if 'FunctionError' in invokeResponse:
            logger.error('%s error: %s', invokeResponse['FunctionError'],
                         invokeResponse['Payload'].read())
