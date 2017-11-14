"""
Note: this file will be copied to the Lambda too. Do not
add dependencies carelessly.
"""

from collections import namedtuple
from requests import request

# These are content encodings that requests decodes automatically
AUTO_DECODED_CONTENTS = {'gzip', 'deflate'}

# Compress the body if the client accepts it
MIN_COMPRESS_SIZE = 4096

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
        if (TRANSFER_ENCODING in responseHeaders and
                    responseHeaders[TRANSFER_ENCODING] == 'chunked'):
            del responseHeaders[TRANSFER_ENCODING]

        if (CONTENT_ENCODING in responseHeaders and
                    responseHeaders[CONTENT_ENCODING]
                in AUTO_DECODED_CONTENTS):
            del responseHeaders[CONTENT_ENCODING]

        if gzipResult and len(responseBody) > MIN_COMPRESS_SIZE:
            if (ACCEPT_ENCODING in responseHeaders and
                        'gzip' in responseHeaders[ACCEPT_ENCODING] and
                        CONTENT_ENCODING not in responseHeaders):
                if (CONTENT_TYPE in responseHeaders and
                            'text' in responseHeaders[CONTENT_TYPE]):
                    responseBody = responseBody.encode('zlib')
                    responseHeaders[CONTENT_ENCODING] = 'gzip'

        responseHeaders[CONTENT_LENGTH] = len(responseBody)

    return ProxyResponse(statusCode=statusCode,
                         headers=responseHeaders,
                         content=responseBody)
