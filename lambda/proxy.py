import base64
import requests


AUTO_DECODED_CONTENTS = {'gzip', 'deflate'}


def handler(event, context):
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']

    print method, url
    for header, value in requestHeaders.iteritems():
        print '  %s: %s' % (header, value)

    if 'body64' in event:
        requestBody = base64.b64decode(event['body64'])
    else:
        requestBody = None

    kwargs = {
        'headers': requestHeaders,
        'allow_redirects': False,
    }
    if requestBody:
        kwargs['data'] = requestBody

    with requests.request(method, url, **kwargs) as response:
        statusCode = response.status_code
        responseHeaders = {k: response.headers[k] for k in response.headers}
        responseBody = response.content

        if 'Transfer-Encoding' in responseHeaders and \
            responseHeaders['Transfer-Encoding'] == 'chunked':
            del responseHeaders['Transfer-Encoding']
            responseHeaders['Content-Length'] = len(responseBody)

        if 'Content-Encoding' in responseHeaders and \
            responseHeaders['Content-Encoding'] in AUTO_DECODED_CONTENTS:
            del responseHeaders['Content-Encoding']
            responseHeaders['Content-Length'] = len(responseBody)

        retVal = {
            'statusCode': statusCode,
            'headers': responseHeaders
        }
        if responseBody:
            retVal['content64'] = base64.b64encode(responseBody)
        return retVal
