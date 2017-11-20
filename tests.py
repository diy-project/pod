#!/usr/bin/env python

import json
import unittest
import os
import random
import sys

from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from threading import Thread

from lib.stats import Stats, ProxyStatsModel

import shared.crypto as crypto
import shared.proxy as proxy

from main import DEFAULT_MAX_LAMBDAS, DEFAULT_PORT, build_local_proxy, \
    build_lambda_proxy, build_handler
from gen_rsa_kp import generate_key_pair


def silence_stdout(func):
    def decorator(*args, **kwargs):
        try:
            with open(os.devnull, 'wb') as devnull:
                sys.stdout = devnull
                func(*args, **kwargs)
        finally:
            sys.stdout = sys.__stdout__
    return decorator


class TestCrypto(unittest.TestCase):

    def test_gcm_encypt_decrypt(self):
        key = 'a' * 16
        cleartext = 'Hello'
        nonce = 'my-nonce'
        ciphertext, tag = crypto.encrypt_with_gcm(key, cleartext, nonce)
        decrypted = crypto.decrypt_with_gcm(key, ciphertext, tag, nonce)
        self.assertEqual(cleartext, decrypted)


def _start_test_server(port, numRequests):

    class Handler(BaseHTTPRequestHandler):

        def log_message(self, format, *args): pass
        def __respond(self, statusCode):
            for header in self.headers:
                if header == 'A': assert self.headers['A'] == '1'
            self.send_response(statusCode)
            self.send_header('B', '2')
            self.end_headers()
            self.wfile.write(TestProxy.EXPECTED_RESPONSE_BODY)
        def do_GET(self): self.__respond(200)
        def do_POST(self):
            body = self.rfile.read(int(self.headers['Content-Length']))
            assert body == TestProxy.EXPECTED_POST_BODY
            self.__respond(201)

    server = HTTPServer(('localhost', port), Handler)
    def run_server():
        for _ in xrange(numRequests):
            server.handle_request()
    t = Thread(target=run_server)
    t.daemon = True
    t.start()


class TestProxy(unittest.TestCase):

    EXPECTED_REQUEST_HEADERS = {'A': '1'}
    EXPECTED_RESPONSE_HEADERS = {'B': '2'}
    EXPECTED_POST_BODY = json.dumps({'request': 'Ping'})
    EXPECTED_RESPONSE_BODY = json.dumps({'response': 'pong'})

    def test_proxy_real_request(self):
        response = proxy.proxy_single_request('GET', 'http://google.com',
                                              {'Connection': 'close'}, None)
        self.assertEqual(response.statusCode, 301,
                         'Response from Google should be redirect')

    def test_proxy_local_request(self):
        port = random.randint(9000, 10000)
        url = 'http://localhost:%d/' % port
        _start_test_server(port, 3)
        response = proxy.proxy_single_request(
            'GET', url, TestProxy.EXPECTED_REQUEST_HEADERS, b'')
        self.assertEqual(response.statusCode, 200)
        self.assertDictContainsSubset(TestProxy.EXPECTED_RESPONSE_HEADERS,
                                      response.headers)
        self.assertEqual(response.content,
                         TestProxy.EXPECTED_RESPONSE_BODY)

        response = proxy.proxy_single_request(
            'GET', url, TestProxy.EXPECTED_REQUEST_HEADERS, None)
        self.assertEqual(response.statusCode, 200)
        self.assertDictContainsSubset(TestProxy.EXPECTED_RESPONSE_HEADERS,
                                      response.headers)
        self.assertEqual(response.content,
                         TestProxy.EXPECTED_RESPONSE_BODY)

        response = proxy.proxy_single_request(
            'POST', url, {
                'Foo': 'Bar',
                'Content-Length': str(len(TestProxy.EXPECTED_POST_BODY))
            },
            TestProxy.EXPECTED_POST_BODY)
        self.assertEqual(response.statusCode, 201)
        self.assertDictContainsSubset(TestProxy.EXPECTED_RESPONSE_HEADERS,
                                      response.headers)
        self.assertEqual(response.content,
                         TestProxy.EXPECTED_RESPONSE_BODY)


class TestRsaKeygen(unittest.TestCase):

    @silence_stdout
    def test_keygen(self):
        generate_key_pair(os.devnull, os.devnull)


class TestBuildProxy(unittest.TestCase):
    """Tries to build the proxies, but not actually run the server."""

    @staticmethod
    def _get_default_setup():
        stats = Stats()
        stats.register_model('proxy', ProxyStatsModel())

        class MockArgs(object):
            pass

        args = MockArgs()
        args.port = DEFAULT_PORT
        args.host = 'localhost'
        args.functions = []
        args.enableEncryption = False
        args.lambdaType = 'short'
        args.s3Bucket = None
        args.publicServerHostAndPort = None
        args.maxLambdas = DEFAULT_MAX_LAMBDAS
        args.enableMitm = False
        args.disableStats = False
        args.verbose = False
        return args, stats, None

    @silence_stdout
    def test_build_local_no_mitm(self):
        args, stats, _ = TestBuildProxy._get_default_setup()
        args.local = True
        args.enableMitm = False
        proxy = build_local_proxy(args, stats)
        build_handler(proxy, stats, verbose=True)

    @silence_stdout
    def test_build_local_with_mitm(self):
        args, stats, _ = TestBuildProxy._get_default_setup()
        args.local = True
        args.enableMitm = True
        proxy = build_local_proxy(args, stats)
        build_handler(proxy, stats, verbose=True)

    @silence_stdout
    def test_build_lambda_with_mitm(self):
        args, stats, reverseServer = TestBuildProxy._get_default_setup()
        args.enableMitm = True
        args.functions = ['proxy']
        args.s3Bucket = 'mock-bucket'
        args.enableEncryption = True
        proxy = build_lambda_proxy(args, stats, reverseServer)
        build_handler(proxy, stats, verbose=True)


if __name__ == '__main__':
    unittest.main()
