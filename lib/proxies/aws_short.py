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

from shared.crypto import REQUEST_META_NONCE, RESPONSE_META_NONCE, \
    REQUEST_BODY_NONCE, RESPONSE_BODY_NONCE, \
    decrypt_with_gcm, encrypt_with_gcm
from shared.proxy import MAX_LAMBDA_BODY_SIZE

logger = logging.getLogger(__name__)

random = SystemRandom()

SESSION_KEY_LENGTH = 16


def _get_region_from_arn(arn):
    elements = arn.split(':')
    return elements[3]


class ShortLivedLambdaProxy(AbstractRequestProxy):
    """Invoke a lambda for each request"""

    def __init__(self, functions, maxParallelRequests, s3Bucket,
                 pubKeyFile, stats):
        self.__functions = functions
        self.__functionToClient = {}
        self.__regionToClient = {}
        self.__lambdaRateSemaphore = Semaphore(maxParallelRequests)
        self.__lambda = boto3.client('lambda')

        if 'lambda' not in stats.models:
            stats.register_model('lambda', LambdaStatsModel())
        self.__lambdaStats = stats.get_model('lambda')

        self.__enableS3 = False
        if s3Bucket is not None:
            self.__s3Bucket = s3Bucket
            if 's3' not in stats.models:
                stats.register_model('s3', S3StatsModel())
            self.__s3Stats = stats.get_model('s3')
            self.__s3 = boto3.client('s3')
            self.__s3DeletePool = ThreadPoolExecutor(1)
            self.__enableS3 = True

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

    def __delete_object_from_s3(self, key):
        assert self.__enableS3 is True
        self.__s3.delete_object(Bucket=self.__s3Bucket, Key=key)

    def __load_object_from_s3(self, key):
        assert self.__enableS3 is True
        result = self.__s3.get_object(Bucket=self.__s3Bucket, Key=key)
        ret = result['Body'].read()
        self.__s3DeletePool.submit(self.__delete_object_from_s3, key)
        self.__s3Stats.record_get(len(ret))
        return ret

    def __put_object_into_s3(self, data):
        assert self.__enableS3 is True
        md5 = hashlib.md5()
        md5.update(data)
        key = md5.hexdigest()
        s3Bucket = boto3.resource('s3').Bucket(self.__s3Bucket)
        s3Bucket.put_object(Key=key, Body=data,
                            StorageClass='REDUCED_REDUNDANCY')
        self.__s3Stats.record_get(len(data))
        return key

    def __prepare_request_body(self, body, sessionKey):
        bodyArgs = {}
        if len(body) <= MAX_LAMBDA_BODY_SIZE:
            if self.__enableEncryption:
                bodyData, bodyTag = encrypt_with_gcm(sessionKey, body,
                                                     REQUEST_BODY_NONCE)
                bodyArgs['bodyTag'] = b64encode(bodyTag)
                bodyArgs['body64'] = b64encode(bodyData)
            else:
                bodyArgs['body64'] = b64encode(body)
        elif self.__enableS3:
            if self.__enableEncryption:
                assert sessionKey is not None
                s3Data, s3Tag = encrypt_with_gcm(sessionKey, body,
                                                 REQUEST_BODY_NONCE)
                bodyArgs['s3Tag'] = b64encode(s3Tag)
            else:
                s3Data = body
            requestS3Key = self.__put_object_into_s3(s3Data)
            bodyArgs['s3Key'] = requestS3Key
        return bodyArgs

    def __prepare_encrypted_metadata(self, metaArgs, sessionKey):
        assert sessionKey is not None
        ciphertext, tag = encrypt_with_gcm(sessionKey,
                                           json.dumps(metaArgs),
                                           REQUEST_META_NONCE)
        key = self.__rsaCipher.encrypt(sessionKey)
        return {
            'meta64': b64encode(ciphertext),
            'metaTag': b64encode(tag),
            'key': b64encode(key)
        }

    def __handle_encrypted_metadata(self, response, sessionKey):
        assert sessionKey is not None
        ciphertext = b64decode(response['meta64'])
        tag = b64decode(response['metaTag'])
        plaintext = decrypt_with_gcm(sessionKey, ciphertext, tag,
                                     RESPONSE_META_NONCE)
        return json.loads(plaintext)

    def __handle_response_body(self, response, sessionKey):
        content = b''
        if 'content64' in response:
            content = b64decode(response['content64'])
            if self.__enableEncryption:
                assert sessionKey is not None
                content = decrypt_with_gcm(sessionKey, content,
                                           b64decode(response['contentTag']),
                                           RESPONSE_BODY_NONCE)
        elif 's3Key' in response:
            content = self.__load_object_from_s3(response['s3Key'])
            if self.__enableEncryption:
                assert sessionKey is not None
                tag = b64decode(response['s3Tag'])
                content = decrypt_with_gcm(sessionKey, content, tag,
                                           RESPONSE_BODY_NONCE)
        return content

    def request(self, method, url, headers, body):
        logger.debug('Proxying %s %s with Lamdba', method, url)
        sessionKey = None
        if self.__enableEncryption:
            sessionKey = get_random_bytes(SESSION_KEY_LENGTH)

        requestS3Key = None
        try:
            invokeArgs = {
                'method': method,
                'url': url,
                'headers': headers,
            }
            if self.__enableS3:
                invokeArgs['s3Bucket'] = self.__s3Bucket
            if self.__enableEncryption:
                invokeArgs = self.__prepare_encrypted_metadata(invokeArgs,
                                                               sessionKey)
            if body is not None:
                invokeArgs.update(self.__prepare_request_body(body, sessionKey))

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
        if self.__enableEncryption:
            responseMeta = (self.__handle_encrypted_metadata(response,
                                                             sessionKey))
            statusCode = responseMeta['statusCode']
            headers = responseMeta['headers']
        else:
            statusCode = response['statusCode']
            headers = response['headers']
        content = self.__handle_response_body(response, sessionKey)
        return ProxyResponse(statusCode=statusCode, headers=headers,
                             content=content)
