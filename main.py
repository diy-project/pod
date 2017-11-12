#!/usr/bin/env python

import argparse
import base64
import errno
import json
import logging
import select
import socket
import sys

from collections import namedtuple
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from SocketServer import ThreadingMixIn

try:
    from fake_useragent import UserAgent
except ImportError, e:
    logging.warn('Failed to import fake_useragent.UserAgent', e)
    UserAgent = None


FILTERED_REQUEST_HEADERS = {
    'proxy-connection',
    'connection',
    'user-agent',
    'upgrade-insecure-requests'
}
FILTERED_RESPONSE_HEADERS = {'connection'}

DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36'


def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=1080,
                        help='Port to listen on')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Address to bind to')
    parser.add_argument('--local', '-l', action='store_true',
                        help='Run the proxy locally')
    parser.add_argument('--function', '-f', dest='functions',
                        action='append', default=['simple-http-proxy'],
                        help='Lambda functions by name or ARN')
    parser.add_argument('--verbose', '-v', action='store_true')
    return parser.parse_args()


ProxyResponse = namedtuple('Response', ['status_code', 'headers', 'content'])


def build_local_proxy():
    """Request the resource locally"""
    import requests

    logging.warn('Running the proxy locally. This provides no privacy!')
    def proxy_request_locally(method, url, headers, body=None):
        kwargs = {
            'headers': headers,
            'allow_redirects': False
        }
        if body:
            kwargs['data'] = body
        response = requests.request(method, url, **kwargs)
        return ProxyResponse(
            status_code=response.status_code,
            headers=response.headers,
            content=response.content)
    return proxy_request_locally


def build_lambda_proxy(functions):
    """Request the resource using lambda"""
    import boto3
    from random import SystemRandom

    logging.info('Running the proxy with Lambda')
    if not functions:
        logging.fatal('No functions specified')
        sys.exit(-1)

    client = boto3.client('lambda')
    secureRandom = SystemRandom()

    def proxy_request_with_lambda(method, url, headers, body=None):
        args = {
            'method': method,
            'url': url,
            'headers': headers,
        }
        if body:
            args['body64'] = base64.b64encode(body)

        response = client.invoke(FunctionName=secureRandom.choice(functions),
                                 Payload=json.dumps(args))
        if response['StatusCode'] != 200:
            logging.error('%s: status=%d', response['FunctionError'], response['StatusCode'])
            return ProxyResponse(status_code=500, headers={}, content='')

        payload = json.loads(response['Payload'].read())
        if 'content64' in payload:
            content = base64.b64decode(payload['content64'])
        else:
            content = ''
        return ProxyResponse(status_code=payload['statusCode'],
                             headers=payload['headers'],
                             content=content)

    return proxy_request_with_lambda

def build_handler(httpProxyFn, verbose):
    """Construct a request handler"""
    if UserAgent:
        ua = UserAgent()
        get_user_agent = lambda: ua.random
    else:
        get_user_agent = lambda: DEFAULT_USER_AGENT

    class ProxyHandler(BaseHTTPRequestHandler):

        def _print_request(self):
            print 'client:', self.client_address
            print 'command:', self.command
            print 'path:', self.path
            print 'version:', self.request_version
            for header in self.headers:
                print '  %s: %s' % (header, self.headers[header])

        def _print_response(self, response):
            print 'url:', self.path
            print 'status:', response.status_code
            for header in response.headers:
                print '  %s: %s' % (header, response.headers[header])

        def _proxy_request(self):
            if verbose: self._print_request()

            method = self.command.lower()
            url = self.path
            headers = {}
            for header in self.headers:
                if header.lower() in FILTERED_REQUEST_HEADERS:
                    continue
                headers[header] = self.headers[header]
            headers['connection'] = 'close'
            headers['user-agent'] = get_user_agent()

            if self.command != 'GET':
                requestBody = self.rfile.read()
            else:
                requestBody = None

            response = httpProxyFn(method, url, headers, requestBody)
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

        @staticmethod
        def _connect_read_write(cliSock, servSock, max_idle_timeout=30):
            rlist = [cliSock, servSock]
            wlist = []
            idleCount = 0
            while True:
                idleCount += 1
                (ins, _, exs) = select.select(rlist, wlist, rlist, 1.0)
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
                        idleCount = 0
                if idleCount == max_idle_timeout: break

        def _connect_request(self):
            if verbose: self._print_request()

            host, port = self.path.split(':')
            try:
                sock = socket.create_connection((host, port))
            except socket.error, e:
                self.send_error(520)
                self.end_headers()
                return

            try:
                self.send_response(200)
                self.send_header('proxy-agent', self.version_string())
                self.send_header('proxy-connection', 'close')
                self.end_headers()
                self._connect_read_write(self.connection, sock)
            except Exception as e:
                logging.error('CONNECT error', e)
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


def main(host, port, functions=None, local=False, verbose=False):
    if local:
        httpProxyFn = build_local_proxy()
    else:
        httpProxyFn = build_lambda_proxy(functions)

    handler = build_handler(httpProxyFn, verbose=verbose)
    server = ThreadedHTTPServer((host, port), handler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()


if __name__ == '__main__':
    main(**vars(get_args()))
