import atexit
import logging
import os
import shutil
import socket
import ssl
import tempfile

from httplib import responses
from OpenSSL import crypto
from random import SystemRandom
from termcolor import colored
from threading import Lock

from ..constants import FILTERED_REQUEST_HEADERS, FILTERED_RESPONSE_HEADERS, \
    DEFAULT_USER_AGENT
from ..proxy import AbstractRequestProxy, AbstractStreamProxy


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class MockSocket(object):
    """Do not use this as an actual socket"""

    def __init__(self, host, port):
        self.host = host
        self.port = port

    def close(self):
        pass

    def send(self, data):
        raise NotImplementedError

    def recv(self, data):
        raise NotImplementedError

    def __str__(self):
        return self.host + ':' + self.port


def _print_mitm_request(method, url, headers):
    print colored('command (https): %s %s' % (method, url), 'white', 'on_red')
    for k, v in headers.iteritems():
        print '  %s: %s' % (k, v)


def _print_mitm_response(url, response):
    print colored('url: %s' % url, 'white', 'on_green')
    print 'status:', response.status_code
    for k, v in response.headers.iteritems():
        print '  %s: %s' % (k, v)
    print 'content-len:', len(response.content)


class MitmHttpsProxy(AbstractStreamProxy):
    """Intercepts a stream and translates it to requests"""

    certCache = {}
    certCacheLock = Lock()
    secureRandom = SystemRandom()

    def __init__(self, requestProxy, certfile, keyfile,
                 overrideUserAgent=False, verbose=False):
        assert isinstance(requestProxy, AbstractRequestProxy)

        # Config
        self.verbose = verbose
        self.overrideUserAgent = overrideUserAgent

        # Single request proxy
        self.baseRequestProxy = requestProxy

        # Load root CA certificate and key
        self.caCert = crypto.load_certificate(crypto.FILETYPE_PEM,
                                         open(certfile).read())
        self.caKey = crypto.load_privatekey(crypto.FILETYPE_PEM,
                                       open(keyfile).read())

        tempCertDir = tempfile.mkdtemp(suffix='mitmproxy')
        atexit.register(lambda: shutil.rmtree(tempCertDir))
        self.tempCertDir = tempCertDir

    def connect(self, host, port):
        return MockSocket(host, port)

    def stream(self, cliSock, servSock):
        certFile, keyFile = self._get_cert_for_host(servSock.host)
        context = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
        context.load_cert_chain(certFile, keyFile)
        cliSslSock = context.wrap_socket(cliSock, server_side=True)
        try:
            self._stream_one_request(cliSslSock, servSock)
        except Exception, e:
            logger.exception(e)
        finally:
            cliSslSock.shutdown(socket.SHUT_RDWR)

    def _sign_cert_for_host(self, host):
        serial = self.secureRandom.getrandbits(32)

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
        cert.set_issuer(self.caCert.get_subject())
        cert.set_pubkey(key)
        cert.sign(self.caKey, 'sha1')

        keyPath = os.path.join(self.tempCertDir,
                               host.replace('.', '_') + '.key')
        certPath = os.path.join(self.tempCertDir,
                                host.replace('.', '_') + '.pem')
        with open(keyPath, 'wb') as ofs:
            ofs.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, key))
        with open(certPath, 'wb') as ofs:
            ofs.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
        return (certPath, keyPath)

    def _get_cert_for_host(self, host):
        with self.certCacheLock:
            if host not in self.certCache:
                resp = self._sign_cert_for_host(host)
                self.certCache[host] = resp
            else:
                resp = self.certCache[host]
        return resp

    def _stream_one_request(self, cliSslSock, servSock):
        # Read until the end of the headers
        data = b''
        while True:
            chunk = cliSslSock.recv(8192)
            if chunk == '':
                raise IOError('Unable to parse request: %s' % servSock)
            data += chunk
            if '\r\n\r\n' in data:
                splitIdx = data.index('\r\n\r\n')
                request = data[:splitIdx]
                data = data[splitIdx + 4:]
                break

        # Parse the headers
        contentLength = 0
        requestLines = request.splitlines()
        method, path, httpVersion = requestLines[0].split(' ', 2)
        url = 'https://%s:%s%s' % (servSock.host, servSock.port, path)
        headers = {}
        for headerLine in requestLines[1:]:
            header, value = headerLine.split(': ', 1)
            if header in FILTERED_REQUEST_HEADERS:
                continue
            if header == 'Content-Length':
                contentLength = int(value)
            headers[header] = value
        headers['Connection'] = 'close'
        if self.overrideUserAgent:
            headers['User-Agent'] = DEFAULT_USER_AGENT

        # Read the rest of the body
        if contentLength > 0:
            while len(data) < contentLength:
                chunk = cliSslSock.recv(8192)
                if chunk == '':
                    raise IOError('Failed to read all data: %s' % servSock)
                data += chunk
        else:
            data = None

        if self.verbose:
            _print_mitm_request(method, url, headers)
        response = self.baseRequestProxy.request(method, url, headers, data)
        if self.verbose:
            _print_mitm_response(url, response)

        responseLines = []
        responseLines.append('%s %d %s' %
                             (httpVersion, response.status_code,
                              responses[response.status_code]))
        for header in response.headers:
            if header in FILTERED_RESPONSE_HEADERS:
                continue
            responseLines.append('%s: %s' % (header, response.headers[header]))
        responseLines.append('Connection: close')
        responseLines.append('')
        responseLines.append('')
        cliSslSock.sendall('\r\n'.join(responseLines))
        if response.content:
            cliSslSock.sendall(response.content)
