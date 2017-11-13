import base64
import requests


def handler(event, context):
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']
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
        if contentEncoding == 'gzip' or contentEncoding == 'deflate' or contentEncoding == 'br':
            responseHeaders['Content-Encoding'] = 'gzip'
            responseBody = responseBody.encode('zlib')
            responseHeaders['Content-Length'] = len(responseBody)

    retVal = {
        'statusCode': statusCode,
        'headers': responseHeaders
    }
    if responseBody:
        retVal['content64'] = base64.b64encode(responseBody)
    return retVal
