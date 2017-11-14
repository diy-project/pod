import errno
import requests
import select
import socket

from ..proxy import AbstractRequestProxy, AbstractStreamProxy, ProxyResponse


AUTO_DECODED_CONTENTS = {'gzip', 'deflate'}


class LocalProxy(AbstractRequestProxy, AbstractStreamProxy):

    def __init__(self, maxIdleTimeout=60):
        self.maxIdleTimeout = maxIdleTimeout

    def request(self, method, url, headers, body):
        kwargs = {
            'headers': headers,
            'allow_redirects': False,
        }
        if body:
            kwargs['data'] = body

        with requests.request(method, url, **kwargs) as response:
            statusCode = response.status_code
            responseHeaders = {k: response.headers[k] for k in response.headers}
            responseBody = response.content

            if ('Transfer-Encoding' in responseHeaders and
                        responseHeaders['Transfer-Encoding'] == 'chunked'):
                del responseHeaders['Transfer-Encoding']
                responseHeaders['Content-Length'] = len(responseBody)

            if ('Content-Encoding' in responseHeaders and
                        responseHeaders['Content-Encoding']
                    in AUTO_DECODED_CONTENTS):
                del responseHeaders['Content-Encoding']
                responseHeaders['Content-Length'] = len(responseBody)

            return ProxyResponse(
                status_code=statusCode,
                headers=responseHeaders,
                content=responseBody)

    def connect(self, host, port):
        sock = socket.create_connection((host, port))
        return sock

    def stream(self, cliSock, servSock):
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
            if idleSecs >= self.maxIdleTimeout: break

