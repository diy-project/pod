#!/usr/bin/env python

import argparse
import logging
import sys

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from fake_useragent import UserAgent
from SocketServer import ThreadingMixIn
from termcolor import colored

from lib.headers import FILTERED_REQUEST_HEADERS, FILTERED_RESPONSE_HEADERS,\
    DEFAULT_USER_AGENT
from lib.proxy import ProxyInstance
from lib.proxies.local import LocalProxy
from lib.proxies.aws import ShortLivedLambdaProxy, LongLivedLambdaProxy
from lib.proxies.mitm import MitmHttpsProxy

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)

DEFAULT_PORT = 1080
DEFAULT_MAX_LAMBDAS = 10

MITM_CERT_PATH = 'mitm.ca.pem'
MITM_KEY_PATH = 'mitm.key.pem'

OVERRIDE_USER_AGENT = False


def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', '-p', type=int, default=DEFAULT_PORT,
                        help='Port to listen on')
    parser.add_argument('--host', type=str, default='localhost',
                        help='Address to bind to')
    parser.add_argument('--local', '-l', action='store_true',
                        dest='runLocal',
                        help='Run the proxy locally')
    parser.add_argument('--function', '-f', dest='functions',
                        action='append', default=['simple-http-proxy'],
                        help='Lambda functions by name or ARN')
    parser.add_argument('--short-lived-lambdas', '-sll',
                        dest='enableShortLivedLambdas', action='store_true',
                        help='Make each lambda a single request/response')
    parser.add_argument('--max-lambdas', '-t', type=int,
                        default=DEFAULT_MAX_LAMBDAS, dest='maxLambdas',
                        help='Max number of lambdas running at any time')
    parser.add_argument('--enable-mitm', '-m', action='store_true',
                        dest='enableMitm',
                        help='Run as a MITM for TLS traffic')
    parser.add_argument('--verbose', '-v', action='store_true')
    return parser.parse_args()


def build_local_proxy(enableMitm, verbose):
    """Request the resource locally"""

    logger.warn('Running the proxy locally. This provides no privacy!')

    localProxy = LocalProxy()
    if enableMitm:
        logger.warn('MITM proxy enabled. This is experimental!')
        mitmProxy = MitmHttpsProxy(localProxy,
                                   MITM_CERT_PATH, MITM_KEY_PATH,
                                   overrideUserAgent=OVERRIDE_USER_AGENT,
                                   verbose=verbose)
        return ProxyInstance(requestProxy=localProxy, streamProxy=mitmProxy)
    else:
        return ProxyInstance(requestProxy=localProxy, streamProxy=localProxy)


def build_lambda_proxy(functions, enableMitm,
                       enableShortLivedLambdas,
                       maxLambdas, verbose):
    """Request the resource using lambda"""

    logger.info('Running the proxy with Lambda')
    if not functions:
        logger.fatal('No functions specified')
        sys.exit(-1)

    if enableShortLivedLambdas:
        logger.info('Using short-lived Lambdas')
        lambdaProxy = ShortLivedLambdaProxy(functions, maxLambdas)
    else:
        logger.info('Using long-lived Lambdas')
        lambdaProxy = LongLivedLambdaProxy(functions, maxLambdas,
                                           verbose)

    if enableMitm:
        mitmProxy = MitmHttpsProxy(lambdaProxy,
                                   MITM_CERT_PATH, MITM_KEY_PATH,
                                   overrideUserAgent=OVERRIDE_USER_AGENT,
                                   verbose=verbose)
        return ProxyInstance(requestProxy=lambdaProxy, streamProxy=mitmProxy)
    else:
        logger.info('HTTPS will use the local proxy')
        localProxy = LocalProxy()
        return ProxyInstance(requestProxy=lambdaProxy, streamProxy=localProxy)


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
            print 'status:', response.statusCode
            for header in response.headers:
                print '  %s: %s' % (header, response.headers[header])
            print 'content-len:', len(response.content)

        def _proxy_request(self):
            if verbose: self._print_request()

            method = self.command.upper()
            url = self.path
            headers = {}
            for header in self.headers:
                if header in FILTERED_REQUEST_HEADERS:
                    continue
                headers[header] = self.headers[header]
            headers['Connection'] = 'close'
            if OVERRIDE_USER_AGENT:
                headers['User-Agent'] = get_user_agent()

            # TODO: which other requests have no bodies?
            if method != 'GET':
                requestBody = self.rfile.read()
            else:
                requestBody = None

            response = proxy.request(method, url, headers, requestBody)
            if verbose: self._print_response(response)

            self.send_response(response.statusCode)
            for header in response.headers:
                if header in FILTERED_RESPONSE_HEADERS:
                    continue
                self.send_header(header, response.headers[header])
            self.send_header('Proxy-Connection', 'close')
            self.end_headers()
            self.wfile.write(response.content)
            return

        def _connect_request(self):
            if verbose: self._print_request()

            host, port = self.path.split(':')
            try:
                sock = proxy.connect(host, port)
            except Exception, e:
                logger.exception(e)
                self.send_error(520)
                self.end_headers()
                return

            try:
                self.send_response(200)
                self.send_header('Proxy-Agent', self.version_string())
                self.send_header('Proxy-Connection', 'close')
                self.end_headers()
                proxy.stream(self.connection, sock)
            except Exception, e:
                logger.exception(e)
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


def main(host, port,
         functions=None,
         enableMitm=False,
         enableShortLivedLambdas=False,
         maxLambdas=DEFAULT_MAX_LAMBDAS,
         runLocal=False,
         verbose=False):

    if runLocal:
        proxy = build_local_proxy(enableMitm, verbose=verbose)
    else:
        proxy = build_lambda_proxy(
            functions=functions,
            enableMitm=enableMitm,
            maxLambdas=maxLambdas,
            enableShortLivedLambdas=enableShortLivedLambdas,
            verbose=verbose)

    handler = build_handler(proxy, verbose=verbose)
    server = ThreadedHTTPServer((host, port), handler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()


if __name__ == '__main__':
    main(**vars(get_args()))
