"""
Note: this file will be copied to the Lambda too. Do not
add dependencies carelessly.
"""

import errno
import select

from collections import namedtuple
from requests import request

# These are content encodings that requests decodes automatically
AUTO_DECODED_CONTENTS = {'gzip', 'deflate'}

# Compress the body if the client accepts it
MIN_COMPRESS_SIZE = 4096

# The body can be up to 6MB, this leaves leeway while estimating the base64
# overhead
MAX_LAMBDA_BODY_SIZE = int(5.8 * 1024 * 1024) / 4 * 3

# Header types
ACCEPT_ENCODING = 'Accept-Encoding'
TRANSFER_ENCODING = 'Transfer-Encoding'
CONTENT_ENCODING = 'Content-Encoding'
CONTENT_LENGTH = 'Content-Length'
CONTENT_TYPE = 'Content-Type'


ProxyResponse = namedtuple('ProxyResponse', ['statusCode', 'headers', 'content'])


def proxy_single_request(method, url, headers, body, gzipResult=False):
    """Proxy a single request using the requests library"""
    kwargs = {
        'headers': headers,
        'allow_redirects': False,
    }
    if body:
        kwargs['data'] = body

    with request(method, url, **kwargs) as response:
        statusCode = response.status_code
        responseHeaders = {k: response.headers[k] for k in response.headers}
        responseBody = response.content

        # TODO: this does not handle nested encoding
        hasContentEncoding = False
        headers_to_remove = []
        for header in responseHeaders.keys():
            if TRANSFER_ENCODING.lower() == header.lower() and responseHeaders[header] == 'chunked':
                headers_to_remove.append(header)

            if CONTENT_ENCODING.lower() == header.lower():
                if responseHeaders[header] in AUTO_DECODED_CONTENTS:
                    headers_to_remove.append(header)
                else:
                    hasContentEncoding = True

        for header in headers_to_remove:
            del responseHeaders[header]

        if gzipResult and len(responseBody) > MIN_COMPRESS_SIZE:
            if (ACCEPT_ENCODING in responseHeaders
                and 'gzip' in responseHeaders[ACCEPT_ENCODING]
                and not hasContentEncoding):
                if (CONTENT_TYPE in responseHeaders
                    and 'text' in responseHeaders[CONTENT_TYPE]):
                    responseBody = responseBody.encode('zlib')
                    responseHeaders[CONTENT_ENCODING] = 'gzip'

        responseHeaders[CONTENT_LENGTH] = len(responseBody)

    return ProxyResponse(statusCode=statusCode,
                         headers=responseHeaders,
                         content=responseBody)


def proxy_sockets(sock1, sock2, idleTimeout):
    bytes1 = 0
    bytes2 = 0
    error = None
    rlist = [sock1, sock2]
    wlist = []
    waitSecs = 0.1
    idleSecs = 0.0

    try:
        while True:
            idleSecs += waitSecs
            (ins, _, exs) = select.select(rlist, wlist, rlist, waitSecs)
            if exs: break
            if ins:
                for i in ins:
                    out = sock1 if i is sock2 else sock2
                    data = i.recv(8192)
                    if data:
                        try:
                            out.send(data)
                            if out is sock1:
                                bytes1 += len(data)
                            else:
                                bytes2 += len(data)
                        except IOError as e:
                            if e.errno == errno.EPIPE:
                                break
                            else:
                                error = e
                                break
                    idleSecs = 0.0
            if idleSecs >= idleTimeout: break
    except Exception as e:
        error = e
    return error, bytes1, bytes2
