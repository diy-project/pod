import os
from socket import create_connection

from concurrent.futures import ThreadPoolExecutor
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

from shared.crypto import PRIVATE_KEY_ENV_VAR
from shared.proxy import proxy_sockets

DEBUG = os.environ.get('VERBOSE', False)

rsaPrivKey = os.environ.get(PRIVATE_KEY_ENV_VAR, None)
RSA_CIPHER = None
if rsaPrivKey is not None:
    RSA_CIPHER = PKCS1_OAEP.new(RSA.importKey(rsaPrivKey.decode('hex')))


ASYNC_EXECUTORS = ThreadPoolExecutor()


def receive_stream_connection_headers(sock):
    data = []
    while True:
        b = sock.recv(1)
        if b == b'':
            raise IOError('Could not parse response')
        data.append(b)
        if ''.join(data[-4:]) == '\r\n\r\n':
            headers = ''.join(data[:len(data) - 4]).splitlines()
            return headers


def connect_stream_server(host, port, socketId):
    sock = create_connection((host, port))
    sock.sendall('CONNECT /%s HTTP/1.1\r\n\r\n' % socketId)
    headers = receive_stream_connection_headers(sock)
    _, statusCode, message = headers[0].split(' ', 2)
    statusCode = int(statusCode)
    if statusCode != 200:
        raise IOError('Stream server refused connection (%d): %s' %
                      (statusCode, message))
    return sock


def stream_handler(event, context):
    """Handle a single request and return it immediately"""

    socketId = event['socketId']
    streamServerHost, streamServerPort = event['streamServer'].split(':')
    streamServerPort = int(streamServerPort)
    host = event['host']
    port = event['port']
    idleTimeout = event['idleTimeout']

    externServerFuture = ASYNC_EXECUTORS.submit(
        lambda: create_connection((host, port)))
    try:
        streamServerSock = connect_stream_server(streamServerHost,
                                                 streamServerPort,
                                                 socketId)
        externServerSock = externServerFuture.result(5)
    except:
        externServerFuture.cancel()
        raise

    # TODO: gracefully handle lambda out of time
    try:
        proxy_sockets(externServerSock, streamServerSock, idleTimeout)
    except:
        pass
    finally:
        externServerSock.close()
        streamServerSock.close()
    return {'status': 'OK'}
