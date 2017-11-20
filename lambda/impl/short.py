import boto3
import hashlib
import json
import os

from base64 import b64encode, b64decode
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP
from requests import post

from shared.crypto import REQUEST_META_NONCE, RESPONSE_META_NONCE, \
    REQUEST_BODY_NONCE, RESPONSE_BODY_NONCE, \
    decrypt_with_gcm, encrypt_with_gcm, PRIVATE_KEY_ENV_VAR
from shared.proxy import proxy_single_request, MAX_LAMBDA_BODY_SIZE

DEBUG = os.environ.get('VERBOSE', False)


S3_RESOURCE = boto3.resource('s3')

rsaPrivKey = os.environ.get(PRIVATE_KEY_ENV_VAR, None)
RSA_CIPHER = None
if rsaPrivKey is not None:
    RSA_CIPHER = PKCS1_OAEP.new(RSA.importKey(rsaPrivKey.decode('hex')))


def decrypt_encrypted_metadata(event):
    encryptedKey = b64decode(event['key'])
    ciphertext = b64decode(event['meta64'])
    tag = b64decode(event['metaTag'])

    sessionKey = RSA_CIPHER.decrypt(encryptedKey)

    cleartext = decrypt_with_gcm(sessionKey, ciphertext, tag,
                                 REQUEST_META_NONCE)
    return sessionKey, json.loads(cleartext)


def decrypt_encrypted_body(event, sessionKey, s3BucketName):
    if 'body64' in event:
        bodyData = b64decode(event['body64'])
        if sessionKey is not None:
            tag = b64decode(event['bodyTag'])
            requestBody = decrypt_with_gcm(sessionKey, bodyData, tag,
                                           REQUEST_BODY_NONCE)
        else:
            requestBody = bodyData
    elif 's3Key' in event:
        assert s3BucketName is not None
        requestBody = get_request_body_from_s3(s3BucketName, event['s3Key'])
        if sessionKey is not None:
            tag = b64decode(event['s3Tag'])
            requestBody = decrypt_with_gcm(sessionKey, requestBody, tag,
                                           REQUEST_BODY_NONCE)
    else:
        requestBody = None
    return requestBody


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


def post_message_to_server(messageServerHostAndPort, messageData):
    md5 = hashlib.md5()
    md5.update(messageData)
    messageId = md5.hexdigest()
    response = post('http://%s/%s' % (messageServerHostAndPort, messageId),
                    headers={
                        'Content-Length': str(len(messageData)),
                        'Content-Type': 'application/binary'},
                    data=messageData)
    if response.status_code != 204:
        raise IOError('Failed to post message to server: %s' %
                      messageServerHostAndPort)
    return messageId


def encrypt_response_metadata(metadata, sessionKey):
    ciphertext, tag = encrypt_with_gcm(sessionKey, json.dumps(metadata),
                                       RESPONSE_META_NONCE)
    return {'meta64': b64encode(ciphertext), 'metaTag': b64encode(tag)}


def prepare_response_content(content, sessionKey, s3BucketName,
                             messageServerHostAndPort):
    ret = {}
    if s3BucketName is not None and len(content) >= MAX_LAMBDA_BODY_SIZE:
        if sessionKey is None:
            s3Data = content
        else:
            s3Data, tag = encrypt_with_gcm(sessionKey, content,
                                           RESPONSE_BODY_NONCE)
            ret['s3Tag'] = b64encode(tag)
        ret['s3Key'] = put_response_body_in_s3(s3BucketName, s3Data)
    elif messageServerHostAndPort is not None \
            and len(content) >= MAX_LAMBDA_BODY_SIZE:
        if sessionKey is None:
            messageData = content
        else:
            messageData, tag = encrypt_with_gcm(sessionKey, content,
                                                RESPONSE_BODY_NONCE)
            ret['messageTag'] = b64encode(tag)
        ret['messageId'] = post_message_to_server(messageServerHostAndPort,
                                                  messageData)
    else:
        if sessionKey is not None:
            data, tag = encrypt_with_gcm(sessionKey,
                                         content,
                                         RESPONSE_BODY_NONCE)
            ret['contentTag'] = b64encode(tag)
            ret['content64'] = b64encode(data)
        else:
            ret['content64'] = b64encode(content)
    return ret


def short_lived_handler(event, context):
    """Handle a single request and return it immediately"""
    if 'key' in event:
        sessionKey, requestMeta = decrypt_encrypted_metadata(event)
    else:
        sessionKey, requestMeta = None, event

    # Unpack request metadata
    method = requestMeta['method']
    url = requestMeta['url']
    requestHeaders = requestMeta['headers']
    s3BucketName = requestMeta.get('s3Bucket', None)
    messageServerHostAndPort = requestMeta.get('messageServer', None)

    # Unpack request body
    requestBody = decrypt_encrypted_body(event, sessionKey, s3BucketName)
    response = proxy_single_request(method, url, requestHeaders,
                                    requestBody, gzipResult=True)
    ret = {
        'statusCode': response.statusCode,
        'headers': response.headers
    }

    if sessionKey is not None:
        ret = encrypt_response_metadata(ret, sessionKey)

    if response.content:
        ret.update(prepare_response_content(response.content, sessionKey,
                                            s3BucketName,
                                            messageServerHostAndPort))
    return ret
