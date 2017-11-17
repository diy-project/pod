#!/usr/bin/env python

import argparse
import proxy
from impl import long


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--short-lived', '-s', dest='shortLived',
                        action='store_true',
                        help='Test the short lived proxy')
    parser.add_argument('--id', type=int,
                        help='Identifier for queues')
    parser.add_argument('--s3bucket', '-s3', type=str,
                        help='Enable S3 for large payloads')
    return parser.parse_args()


class DummyContext(object):
    def get_remaining_time_in_millis(self):
        return long.MIN_MILLIS_REMAINING + 1


def main():
    """Basic local testing"""
    args = get_args()

    if args.shortLived:
        event = {
            'url': 'http://google.com/',
            'method': 'GET',
            'headers': {}
        }
        print proxy.handler(event, DummyContext())
    else:
        assert args.id is not None
        event = {
            'longLived': True,
            'workerId': 0,
            'taskQueue': 'lambda-proxy_task_%d' % args.id,
            'resultQueue': 'lambda-proxy_result_%d' % args.id
        }
        if args.s3bucket:
            event['s3Bucket'] = args.s3bucket

        proxy.DEBUG = True
        long.DEBUG = True

        while True:
            print proxy.handler(event, DummyContext())


if __name__ == '__main__':
    main()
