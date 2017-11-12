import base64
import requests


def proxy_request(method, url, headers, body=None):
    kwargs = {
        'headers': headers,
        'allow_redirects': False
    }
    if body:
        kwargs['data'] = body
    return requests.request(method, url, **kwargs)


def handler(event, context):
    method = event['method']
    url = event['url']
    requestHeaders = event['headers']
    if 'body64' in event:
        requestBody = base64.b64decode(event['body64'])
    else:
        requestBody = None

    response = proxy_request(method, url, requestHeaders, requestBody)
    responseBody = response.content

    retVal = {
        'statusCode': response.status_code,
        'headers': {k: response.headers[k] for k in response.headers}
    }
    if responseBody:
        retVal['content64'] = base64.b64encode(responseBody)
    return retVal
