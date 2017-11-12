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
        'allow_redirects': False
    }
    if requestBody:
        kwargs['data'] = requestBody

    response = requests.request(method, url, **kwargs)
    responseBody = response.content
    retVal = {
        'statusCode': response.status_code,
        'headers': {k: response.headers[k] for k in response.headers}
    }

    if responseBody:
        retVal['content64'] = base64.b64encode(responseBody)
    return retVal
