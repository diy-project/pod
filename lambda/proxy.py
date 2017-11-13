import base64
import requests


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
        'stream': True
    }
    if requestBody:
        kwargs['data'] = requestBody

    with requests.request(method, url, **kwargs) as response:
        statusCode = response.status_code
        responseHeaders = {k: response.headers[k] for k in response.headers}
        responseBody = b''
        if 'Response-Encoding' in responseHeaders and \
            responseHeaders['Response-Encoding'] == 'chunked':
            for chunk in response.iter_content(chunk_size=None):
                responseBody += chunk
            responseHeaders['Content-Length'] = len(responseBody)
            del responseHeaders['Response-Encoding']
        elif 'Content-Length' in responseHeaders:
            contentLength = int(responseHeaders['Content-Length'])
            responseBody = response.raw.read(contentLength)
        else:
            responseBody = response.raw.read()

        retVal = {
            'statusCode': statusCode,
            'headers': responseHeaders
        }
        if responseBody:
            retVal['content64'] = base64.b64encode(responseBody)
        print retVal
        return retVal
