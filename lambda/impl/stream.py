import os
from socket import create_connection

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from shared.crypto import PRIVATE_KEY_ENV_VAR
from shared.proxy import proxy_sockets

DEBUG = os.environ.get('VERBOSE', False)

rsaPrivKey = os.environ.get(PRIVATE_KEY_ENV_VAR, None)
RSA_CIPHER = None
if rsaPrivKey is not None:
    RSA_CIPHER = PKCS1_OAEP.new(RSA.importKey(rsaPrivKey.decode('hex')))


def stream_handler(event, context):
    """Handle a single request and return it immediately"""

    socketId = event['socketId']
    streamServerHost, streamServerPort = event['streamServer'].split(':')
    host = event['host']
    port = event['port']
    idleTimeout = event['idleTimeout']

    externServerSock = create_connection((host, port))
    streamServerSock = create_connection((streamServerHost,
                                          int(streamServerPort)))
    streamServerSock.sendall('CONNECT /%s HTTP/1.1\r\n\r\n' % socketId)
    data = []
    while True:
        b = streamServerSock.recv(1)
        if b == b'':
            raise IOError('Could not parse response')
        data.append(b)
        if ''.join(data[-4:]) == '\r\n\r\n':
            headers = ''.join(data[:len(data) - 4]).splitlines()
            break
    _, statusCode, message = headers[0].split(' ', 2)
    statusCode = int(statusCode)
    if statusCode != 200:
        raise IOError('Stream server refused connection (%d): %s' %
                      (statusCode, message))
    proxy_sockets(externServerSock, streamServerSock, idleTimeout)
    return {'status': 'OK'}
