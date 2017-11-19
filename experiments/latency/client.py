#!/usr/bin/env python

import argparse

from subprocess import check_output

from collections import OrderedDict


NUM_TRIALS_PER_SIZE = 5

MAX_SIZE_TO_REQUEST = 64 * 1024 * 1024


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('hostAndPort', type=str)
    parser.add_argument('enableProxy', '-p', action='store_true')
    return parser.parse_args()


def single_measurement(hostAndPort, size, enableProxy):
    args = [
        'curl', '-o', '/dev/null', '-s',
        '-w', '"%{time_connect} + %{time_starttransfer} = %{time_total}\n"'
    ]
    if enableProxy:
        args.extend(['-x', 'localhost'])
    args.append('%s/%d' % (hostAndPort, size))
    result = check_output(args)
    print result
    return 0


def take_measurements(hostAndPort, enableProxy):
    size = 0
    results = OrderedDict
    while True:
        if size > MAX_SIZE_TO_REQUEST:
            break
        resultsForSize = []
        for _ in xrange(NUM_TRIALS_PER_SIZE):
            resultsForSize.append(single_measurement(hostAndPort, enableProxy))
        results[size] = resultsForSize
        size *= 2
    return results


def main(args):
    results = take_measurements(args.hostAndPort, args.enableProxy)


if __name__ == '__main__':
    main(get_args())