#!/usr/bin/env python

import argparse
import proxy


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('id', type=int,
                        help='Identifier for queues')
    parser.add_argument('--s3bucket', '-s3', type=str,
                        help='Enable S3 for large payloads')
    return parser.parse_args()


def main():
    """Basic local testing"""
    args = get_args()

    event = {
        'longLived': True,
        'workerId': 0,
        'taskQueue': 'lambda-proxy_task_%d' % args.id,
        'resultQueue': 'lambda-proxy_result_%d' % args.id
    }
    if args.s3bucket:
        event['s3Bucket'] = args.s3bucket

    proxy.DEBUG = True

    class DummyContext(object):
        def get_remaining_time_in_millis(self):
            return proxy.MIN_MILLIS_REMAINING + 1

    context = DummyContext()
    while True:
        print proxy.handler(event, context)


if __name__ == '__main__':
    main()
