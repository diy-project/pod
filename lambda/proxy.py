"""Proxy requests using AWS Lambda"""

long_lived_handler = None
short_lived_handler = None
stream_handler = None

def handler(event, context):
    if 'longLived' in event and event['longLived'] == True:
        global long_lived_handler
        if long_lived_handler is None:
            from impl.long import long_lived_handler as llh
            long_lived_handler = llh
        return long_lived_handler(event, context)
    elif 'stream' in event and event['stream'] == True:
        global stream_handler
        if stream_handler is None:
            from impl.stream import stream_handler as sh
            stream_handler = sh
        return stream_handler(event, context)
    else:
        global short_lived_handler
        if short_lived_handler is None:
            from impl.short import short_lived_handler as slh
            short_lived_handler = slh
        return short_lived_handler(event, context)
