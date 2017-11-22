#!/usr/bin/env python
"""
Client that requests a page many times in parallel, forcing the proxy to
trigger multiple lambdas.
"""

import argparse
import sys

from threading import Thread, Lock
from subprocess import check_output


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('hostAndPort', type=str)
    parser.add_argument('--num-requests', '-n', type=int, default=10, dest='n',
                        help='Number of parallel requests to make')
    return parser.parse_args()


def single_request(hostAndPort):
    args = [
        'curl', '-s', '-x', 'localhost', hostAndPort
    ]
    ip, port = check_output(args).split(':')
    return ip


def take_measurements(hostAndPort, n):
    print >> sys.stderr, 'Hitting %s %d times' % (hostAndPort, n)
    ipSet = set()
    ipSetLock = Lock()

    def single_thread():
        try:
            ip = single_request(hostAndPort)
            with ipSetLock:
                ipSet.add(ip)
        except Exception, e:
            print >> sys.stderr, e

    threads = []
    for _ in xrange(n):
        t = Thread(target=single_thread)
        t.start()
        threads.append(t)
    for t in threads: t.join()
    return ipSet


def main(args):
    uniqueIPs = take_measurements(args.hostAndPort, args.n)
    for ip in uniqueIPs:
        print ip
    print 'N: %d, Unique IPs: %d' % (args.n, len(uniqueIPs))


if __name__ == '__main__':
    main(get_args())
