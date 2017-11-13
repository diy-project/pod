#!/usr/bin/env python

import argparse
import base64
import errno
import json
import logging
import os
import select
import socket
import sys

from collections import namedtuple
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn
from termcolor import colored

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

try:
    from fake_useragent import UserAgent
except ImportError, e:
    logger.warn('Failed to import fake_useragent.UserAgent', e)
    UserAgent = None

FILTERED_REQUEST_HEADERS = {
    'proxy-connection',
    'connection',
    'user-agent',
    'upgrade-insecure-requests'
}
FILTERED_RESPONSE_HEADERS = {'connection'}

DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'

MITM_CERT_PATH = 'mitm.ca.pem'
MITM_KEY_PATH = 'mitm.key.pem'


def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=1080,
                        help='Port to listen on')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Address to bind to')
    parser.add_argument('--local', '-l', action='store_true',
                        dest='runLocal',
                        help='Run the proxy locally')
    parser.add_argument('--function', '-f', dest='functions',
                        action='append', default=['simple-http-proxy'],
                        help='Lambda functions by name or ARN')
    parser.add_argument('--enable-mitm', '-m', action='store_true',
                        dest='enableMitm',
                        help='Run as a MITM for TLS traffic')
    parser.add_argument('--verbose', '-v', action='store_true')
    return parser.parse_args()


Proxy = namedtuple('Proxy', ['request', 'connect', 'stream'])
ProxyResponse = namedtuple('Response', ['status_code', 'headers', 'content'])


def build_local_proxy():
    """Request the resource locally"""
    import requests

    logger.warn('Running the proxy locally. This provides no privacy!')
    def proxy_request_locally(method, url, headers, body=None):
        kwargs = {
            'headers': headers,
            'allow_redirects': False
        }
        if body:
            kwargs['data'] = body
        response = requests.request(method, url, **kwargs)
        statusCode = response.status_code
        responseHeaders = {k: response.headers[k] for k in response.headers}
        responseBody = response.content
        if 'Transfer-Encoding' in responseHeaders:
            responseHeaders['Orig-Transfer-Encoding'] = responseHeaders['Transfer-Encoding']
            del responseHeaders['Transfer-Encoding']
        if 'Content-Encoding' in responseHeaders:
            contentEncoding = responseHeaders['Content-Encoding']
            responseHeaders['Orig-Content-Encoding'] = contentEncoding
            # TODO: what if more than one encoding
            if contentEncoding == 'gzip' or contentEncoding == 'deflate':
                del responseHeaders['Content-Encoding']
        responseHeaders['Content-Length'] = len(responseBody) if responseBody else 0

        return ProxyResponse(
            status_code=statusCode,
            headers=responseHeaders,
            content=responseBody)

    def connect(host, port):
        sock = socket.create_connection((host, port))
        return sock

    def stream(cliSock, servSock, max_idle_timeout=60):
        rlist = [cliSock, servSock]
        wlist = []
        waitSecs = 1.0
        idleSecs = 0.0
        while True:
            idleSecs += waitSecs
            (ins, _, exs) = select.select(rlist, wlist, rlist, waitSecs)
            if exs: break
            if ins:
                for i in ins:
                    out = cliSock if i is servSock else servSock
                    data = i.recv(8192)
                    if data:
                        try:
                            out.send(data)
                        except IOError, e:
                            if e.errno == errno.EPIPE:
                                break
                            else:
                                raise
                    idleSecs = 0.0
            if idleSecs >= max_idle_timeout: break

    return Proxy(request=proxy_request_locally, connect=connect,
                 stream=stream)


def build_mitm_lambda_proxy(request_proxy, verbose):
    """Present a self-signed cert to the clieWnt"""
    logger.warn('MITM proxy enabled. This is experimental!')

    import atexit
    import hashlib
    import shutil
    import ssl
    import tempfile

    from httplib import responses
    from OpenSSL import crypto
    from threading import Lock

    caCert = crypto.load_certificate(crypto.FILETYPE_PEM,
                                     open(MITM_CERT_PATH).read())
    caKey = crypto.load_privatekey(crypto.FILETYPE_PEM,
                                   open(MITM_KEY_PATH).read())

    tempCertDir = tempfile.mkdtemp(suffix='mitmproxy')
    atexit.register(lambda: shutil.rmtree(tempCertDir))

    certCache = {}
    certCacheLock = Lock()
    def get_cert_for_host(host):
        def generate_cert_for_host(host):
            md5_hash = hashlib.md5()
            md5_hash.update(host)
            serial = int(md5_hash.hexdigest(), 36)

            key = crypto.PKey()
            key.generate_key(crypto.TYPE_RSA, 2048)

            cert = crypto.X509()
            cert.get_subject().C = 'US'
            cert.get_subject().ST = 'California'
            cert.get_subject().L = 'Palo Alto'
            cert.get_subject().O = 'Stanford University'
            cert.get_subject().OU = 'MITM Proxy'
            cert.get_subject().CN = host
            cert.gmtime_adj_notBefore(0)
            cert.gmtime_adj_notAfter(24 * 60 * 60)
            cert.set_serial_number(serial)
            cert.set_issuer(caCert.get_subject())
            cert.set_pubkey(key)
            cert.sign(caKey, 'sha1')

            keyPath = os.path.join(tempCertDir,
                                   host.replace('.','_') + '.key')
            certPath = os.path.join(tempCertDir,
                                   host.replace('.','_') + '.pem')
            with open(keyPath, 'wb') as ofs:
                ofs.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))
            with open(certPath, 'wb') as ofs:
                ofs.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
            return (certPath, keyPath)

        with certCacheLock:
            if host not in certCache:
                certCache[host] = generate_cert_for_host(host)
            return certCache[host]


    class SockWrapper(object):
        def __init__(self, host, port):
            self.host = host
            self.port = port

        def close(self):
            pass

    def connect(host, port):
        return SockWrapper(host, port)

    def _print_mitm_request(method, url, headers):
        print colored('command (https): %s %s' % (method, url), 'white', 'on_red')
        for k, v in headers.iteritems():
            print '  %s: %s' % (k, v)

    def _print_mitm_response(url, response):
        print colored('url: %s' % url, 'white', 'on_green')
        print 'status:', response.status_code
        for k, v in response.headers.iteritems():
            print '  %s: %s' % (k, v)

    def stream(cliSock, servSock):
        certFile, keyFile = get_cert_for_host(servSock.host)
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        context.load_cert_chain(certFile, keyFile)
        cliSslSock = context.wrap_socket(cliSock, server_side=True)

        # Read until the end of the headers
        data = ''
        request = None
        while True:
            chunk = cliSslSock.recv(8192)
            if chunk == '':
                break
            data += chunk
            if '\r\n\r\n' in data:
                splitIdx = data.index('\r\n\r\n')
                request = data[:splitIdx]
                data = data[splitIdx + 4:]
                break

        # Parse the headers
        contentLength = 0
        if request is None:
            raise RuntimeError('No request received')
        requestLines = request.splitlines()
        method, path, httpVersion = requestLines[0].split(' ', 2)
        url = 'https://%s:%s%s' % (servSock.host, servSock.port, path)
        headers = {}
        for headerLine in requestLines[1:]:
            header, value = headerLine.split(': ', 1)
            headerLower = header.lower()
            if headerLower in FILTERED_REQUEST_HEADERS:
                continue
            elif headerLower == 'content-length':
                contentLength = int(value)
            else:
                headers[header] = value
        headers['connection'] = 'close'
        headers['user-agent'] = DEFAULT_USER_AGENT

        # Read the rest of the body
        if contentLength > 0:
            while len(data) < contentLength:
                chunk = cliSslSock.recv(8192)
                if chunk == '':
                    break
                data += chunk
        else:
            data = None

        if verbose: _print_mitm_request(method, url, headers)
        response = request_proxy(method, url, headers, data)
        if verbose: _print_mitm_response(url, response)

        responseLines = []
        responseLines.append('HTTP/1.1 %d %s' %
                             (response.status_code,
                              responses[response.status_code]))
        if 'Server' in response.headers:
            responseLines.append('Server: %s' % response.headers['Server'])
        if 'Date' in response.headers:
            responseLines.append('Date: %s' % response.headers['Date'])
        for header in response.headers:
            headerLower = header.lower()
            if headerLower in FILTERED_RESPONSE_HEADERS:
                continue
            if headerLower == 'date' or headerLower == 'server':
                continue
            responseLines.append('%s: %s' % (header, response.headers[header]))
        responseLines.append('Connection: close')
        responseLines.append('')
        cliSslSock.sendall('\r\n'.join(responseLines))
        if response.content: cliSslSock.sendall(response.content)
        cliSslSock.shutdown(socket.SHUT_RDWR)

    return Proxy(request=None, connect=connect, stream=stream)


def build_lambda_proxy(functions, enableMitm, verbose):
    """Request the resource using lambda"""
    import boto3
    from random import SystemRandom

    logger.info('Running the proxy with Lambda')
    if not functions:
        logger.fatal('No functions specified')
        sys.exit(-1)

    secureRandom = SystemRandom()
    lambda_ = boto3.client('lambda')

    def proxy_request_with_lambda(method, url, headers, body=None):
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if body:
            args['body64'] = base64.b64encode(body)

        response = lambda_.invoke(FunctionName=secureRandom.choice(functions),
                                  Payload=json.dumps(args))
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
    if enableMitm:
        streamProxy = build_mitm_lambda_proxy(proxy_request_with_lambda, verbose)
    else:
        streamProxy = build_local_proxy()
    return Proxy(request=proxy_request_with_lambda,
                 connect=streamProxy.connect, stream=streamProxy.stream)


def build_handler(proxy, verbose):
    """Construct a request handler"""
    if UserAgent:
        ua = UserAgent()
        get_user_agent = lambda: ua.random
    else:
        get_user_agent = lambda: DEFAULT_USER_AGENT

    class ProxyHandler(BaseHTTPRequestHandler):

        def _print_request(self):
            print colored('command (http): %s %s' % (self.command, self.path),
                          'white', 'on_blue')
            for header in self.headers:
                print '  %s: %s' % (header, self.headers[header])

        def _print_response(self, response):
            print colored('url: %s' % self.path, 'white', 'on_yellow')
            print 'status:', response.status_code
            for header in response.headers:
                print '  %s: %s' % (header, response.headers[header])

        def _proxy_request(self):
            if verbose: self._print_request()

            method = self.command.upper()
            url = self.path
            headers = {}
            for header in self.headers:
                if header.lower() in FILTERED_REQUEST_HEADERS:
                    continue
                headers[header] = self.headers[header]
            headers['connection'] = 'close'
            headers['user-agent'] = get_user_agent()

            if method != 'GET':
                requestBody = self.rfile.read()
            else:
                requestBody = None

            response = proxy.request(method, url, headers, requestBody)
            if verbose: self._print_response(response)

            self.send_response(response.status_code)
            for header in response.headers:
                if header.lower() in FILTERED_RESPONSE_HEADERS:
                    continue
                self.send_header(header, response.headers[header])
            self.send_header('proxy-connection', 'close')
            self.end_headers()
            self.wfile.write(response.content)
            return

        def _connect_request(self):
            if verbose: self._print_request()

            host, port = self.path.split(':')
            try:
                sock = proxy.connect(host, port)
            except Exception, e:
                logger.error(e)
                self.send_error(520)
                self.end_headers()
                return

            try:
                self.send_response(200)
                self.send_header('proxy-agent', self.version_string())
                self.send_header('proxy-connection', 'close')
                self.end_headers()
                proxy.stream(self.connection, sock)
            except Exception as e:
                logger.error('CONNECT failed: %s', e)
                raise
            finally:
                sock.close()
            return

        do_GET = _proxy_request
        do_POST = _proxy_request
        do_HEAD = _proxy_request
        do_DELETE = _proxy_request
        do_PUT = _proxy_request
        do_PATCH = _proxy_request
        do_OPTIONS = _proxy_request
        do_CONNECT = _connect_request

    return ProxyHandler


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


def main(host, port, functions=None, enableMitm=False,
         runLocal=False, verbose=False):
    if runLocal:
        proxy = build_local_proxy()
    else:
        proxy = build_lambda_proxy(functions, enableMitm, verbose)

    handler = build_handler(proxy, verbose=verbose)
    server = ThreadedHTTPServer((host, port), handler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()


if __name__ == '__main__':
    main(**vars(get_args()))
