"""Proxy requests using AWS Lambda"""
import boto3
import hashlib
import json
import os

from base64 import b64encode, b64decode
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from shared.crypto import REQUEST_NONCE, RESPONSE_NONCE, S3_REQUEST_NONCE,\
    S3_RESPONSE_NONCE, decrypt_with_gcm, encrypt_with_gcm, PRIVATE_KEY_ENV_VAR
from shared.proxy import proxy_single_request, MAX_LAMBDA_BODY_SIZE

DEBUG = os.environ.get('VERBOSE', False)


S3_RESOURCE = boto3.resource('s3')

rsaPrivKey = os.environ.get(PRIVATE_KEY_ENV_VAR, None)
RSA_CIPHER = None
if rsaPrivKey is not None:
    RSA_CIPHER = PKCS1_OAEP.new(RSA.importKey(rsaPrivKey.decode('hex')))


def decrypt_encrypted_event(event):
    encryptedKey = b64decode(event['key'])
    ciphertext = b64decode(event['ciphertext'])
    tag = b64decode(event['tag'])

    sessionKey = RSA_CIPHER.decrypt(encryptedKey)

    cleartext = decrypt_with_gcm(sessionKey, ciphertext, tag, REQUEST_NONCE)
    return sessionKey, json.loads(cleartext)


def get_request_body_from_s3(bucketName, key):
    s3Object = S3_RESOURCE.Object(Bucket=bucketName, Key=key)
    return s3Object.get()['Body'].read()


def put_response_body_in_s3(bucketName, data):
    md5 = hashlib.md5()
    md5.update(data)
    key = md5.hexdigest()
    s3Bucket = S3_RESOURCE.Bucket(bucketName)
    s3Bucket.put_object(Key=key, Body=data,
                        StorageClass='REDUCED_REDUNDANCY')
    return key


def short_lived_handler(event, context):
    """Handle a single request and return it immediately"""
    if 'ciphertext' in event:
        sessionKey, event = decrypt_encrypted_event(event)
    else:
        sessionKey = None

    method = event['method']
    url = event['url']
    requestHeaders = event['headers']
    s3BucketName = event.get('s3Bucket', None)

    if 'body64' in event:
        requestBody = b64decode(event['body64'])
    elif 's3Key' in event:
        if s3BucketName is not None:
            requestBody = get_request_body_from_s3(s3BucketName, event['s3Key'])
            if sessionKey is not None:
                tag = b64decode(event['s3Tag'])
                requestBody = decrypt_with_gcm(sessionKey, requestBody, tag,
                                               S3_REQUEST_NONCE)
        else:
            return {'statusCode': 500, 'headers': {}}
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
        if not s3BucketName or len(content64) < MAX_LAMBDA_BODY_SIZE:
            ret['content64'] = content64
        else:
            if sessionKey is None:
                s3Data = response.content
            else:
                s3Data, tag = encrypt_with_gcm(sessionKey,
                                               response.content,
                                               S3_RESPONSE_NONCE)
                ret['s3Tag'] = b64encode(tag)
            ret['s3Key'] = put_response_body_in_s3(s3BucketName, s3Data)

    if sessionKey is None:
        return ret
    else:
        ciphertext, tag = encrypt_with_gcm(sessionKey, json.dumps(ret),
                                           RESPONSE_NONCE)
        return {'ciphertext': b64encode(ciphertext), 'tag': b64encode(tag)}


def handler(event, context):
    if 'longLived' in event and event['longLived'] == True:
        from impl.long import long_lived_handler
        return long_lived_handler(event, context)
    else:
        return short_lived_handler(event, context)
