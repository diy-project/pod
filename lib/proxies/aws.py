import boto3
import hashlib
import json
import logging
from base64 import b64encode, b64decode
from random import SystemRandom
from threading import Semaphore

from concurrent.futures import ThreadPoolExecutor
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from Crypto.Random import get_random_bytes
from lib.proxy import AbstractRequestProxy, ProxyResponse
from lib.stats import LambdaStatsModel, S3StatsModel
from lib.workers import LambdaSqsTaskConfig, LambdaSqsTask, WorkerManager

from shared.crypto import *
from shared.proxy import MAX_LAMBDA_BODY_SIZE

logger = logging.getLogger(__name__)

random = SystemRandom()

SESSION_KEY_LENGTH = 16


def _get_region_from_arn(arn):
    elements = arn.split(':')
    return elements[3]


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    __lambda = boto3.client('lambda')

    def __init__(self, functions, maxParallelRequests, s3Bucket,
                 pubKeyFile, stats):
        self.__functions = functions
        self.__functionToClient = {}
        self.__regionToClient = {}
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)
        self.__s3Bucket = s3Bucket

        # Enable encryption
        self.__rsaPubKey = None
        if pubKeyFile is not None:
            with open(pubKeyFile, 'rb') as ifs:
                self.__rsaPubKey = RSA.importKey(ifs.read())

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

    def __prepare_request_body(self, invokeArgs, body, sessionKey):
        encodedBody = b64encode(body)
        if len(encodedBody) <= MAX_LAMBDA_BODY_SIZE:
            invokeArgs['body64'] = encodedBody
        elif self.__s3Bucket is not None:
            if sessionKey is not None:
                s3Data, s3Tag = encrypt_with_gcm(sessionKey, body,
                                                 S3_REQUEST_NONCE)
                invokeArgs['s3Tag'] = b64encode(s3Tag)
            else:
                s3Data = body
            requestS3Key = self.__put_object_into_s3(s3Data)
            invokeArgs['s3Key'] = requestS3Key

    def __prepare_encrypted_request(self, invokeArgs, sessionKey):
        ciphertext, tag = encrypt_with_gcm(sessionKey,
                                           json.dumps(invokeArgs),
                                           REQUEST_NONCE)
        cipher = PKCS1_OAEP.new(self.__rsaPubKey)
        key = cipher.encrypt(sessionKey)
        return {
            'ciphertext': b64encode(ciphertext),
            'tag': b64encode(tag),
            'key': b64encode(key)
        }

    def __handle_encrypted_response(self, response, sessionKey):
        ciphertext = b64decode(response['ciphertext'])
        tag = b64decode(response['tag'])
        plaintext = decrypt_with_gcm(sessionKey, ciphertext, tag,
                                     RESPONSE_NONCE)
        return json.loads(plaintext)

    def __handle_response_body(self, response, symmetricKey):
        content = b''
        if 'content64' in response:
            content = b64decode(response['content64'])
        elif 's3Key' in response:
            content = self.__load_object_from_s3(response['s3Key'])
            if symmetricKey is not None:
                tag = b64decode(response['s3Tag'])
                content = decrypt_with_gcm(symmetricKey, content, tag,
                                           S3_RESPONSE_NONCE)
        return content

    def request(self, method, url, headers, body):
        logger.debug('Proxying %s %s with Lamdba', method, url)
        sessionKey = None
        if self.__rsaPubKey is not None:
            sessionKey = get_random_bytes(SESSION_KEY_LENGTH)

        requestS3Key = None
        try:
            invokeArgs = {
                'method': method,
                'url': url,
                'headers': headers,
            }
            if body is not None:
                self.__prepare_request_body(invokeArgs, body, sessionKey)
            if self.__s3Bucket is not None:
                invokeArgs['s3Bucket'] = self.__s3Bucket

            if sessionKey is not None:
                invokeArgs = self.__prepare_encrypted_request(invokeArgs,
                                                              sessionKey)

            function = random.choice(self.__functions)
            lambdaClient = self.__get_lambda_client(function)

            self.__lambdaRateSemaphore.acquire()
            try:
                with self.__lambdaStats.record() as billingObject:
                    invokeResponse = lambdaClient.invoke(
                        FunctionName=function,
                        Payload=json.dumps(invokeArgs),
                        LogType='Tail')
                    billingObject.parse_log(invokeResponse['LogResult'])
            finally:
                self.__lambdaRateSemaphore.release()
        finally:
            if requestS3Key is not None:
                self.__s3DeletePool.submit(self.__delete_object_from_s3,
                                           requestS3Key)

        if invokeResponse['StatusCode'] != 200:
            logger.error('%s: status=%d', invokeResponse['FunctionError'],
                         invokeResponse['StatusCode'])
            return ProxyResponse(statusCode=500, headers={}, content='')
        if 'FunctionError' in invokeResponse:
            logger.error('%s error: %s', invokeResponse['FunctionError'],
                         invokeResponse['Payload'].read())
            return ProxyResponse(statusCode=500, headers={}, content='')

        response = json.loads(invokeResponse['Payload'].read())
        if sessionKey is not None:
            response = self.__handle_encrypted_response(response, sessionKey)

        statusCode = response['statusCode']
        headers = response['headers']
        content = self.__handle_response_body(response, sessionKey)
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
