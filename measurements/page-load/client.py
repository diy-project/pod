#!/usr/bin/env python
"""Measure full page load times with headless chrome"""

import argparse
import json
import os
import shutil
import sys
import time

import numpy as np

from collections import OrderedDict
from subprocess import check_call
from urlparse import urlparse

SECONDS_BETWEEN_REQUESTS = 1

SCREENSHOTS_DIR = 'screenshots'


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('url', type=str, help='Url to fetch or a file of urls '
                                              'one per line')
    parser.add_argument('--num-trials', '-n', type=int, dest='n', default=10,
                        help='Number of trials')
    parser.add_argument('--proxy', '-p', type=str, dest='proxyHostAndPort',
                        help='Host and port of the proxy to use')
    parser.add_argument('--outfile', '-o', type=str, dest='outfile',
                        help='Save the results to file as json')
    return parser.parse_args()


def single_measurement(url, proxyHostAndPort, screenshot=False):
    args = [
        'google-chrome',
        '--headless',
        '--disable-gpu',
        '--disk-cache-dir=/dev/null',
        '--download-whole-document',
        '--deterministic-fetch',
        '--window-size=1920,1080',
    ]
    if screenshot is True:
        args.append('--screenshot')
    if proxyHostAndPort is not None:
        args.append('--proxy-server=%s' % proxyHostAndPort)
    args.append(url)
    with open(os.devnull, 'w') as devnull:
        startTime = time.time()
        check_call(args, stdout=devnull, stderr=devnull)
        finTime = time.time() - startTime
    return finTime * 1000


def take_measurements(urls, n, proxyHostAndPort):
    results = OrderedDict()
    for url in urls:
        # request once to prime DNS cache
        try:
            single_measurement(url, proxyHostAndPort)
        except:
            pass
        time.sleep(SECONDS_BETWEEN_REQUESTS)

        parsedUrl = urlparse(url)
        measurements = []
        i = 0
        numFailures = 0
        backoff = SECONDS_BETWEEN_REQUESTS
        while i < n:
            print '%s: %d/%d (%d failures)\r' % (url, i, n, numFailures),
            sys.stdout.flush()
            try:
                measurements.append(single_measurement(url, proxyHostAndPort,
                                                       True))
                shutil.move('screenshot.png',
                            os.path.join(SCREENSHOTS_DIR,
                                         '%s-%d.png' % (parsedUrl.hostname, i)))
                backoff = SECONDS_BETWEEN_REQUESTS
                i += 1
            except:
                numFailures +=1
                backoff *= 2
            time.sleep(backoff)
        print '%s: %d/%d (%d failures)' % (url, i, n, numFailures)
        sys.stdout.flush()
        results[url] = measurements
        if len(measurements) > 0:
            print '  mean=%.01fms, std=%.01fms, median=%.01fms, min=%.01fms, max=%.01fms, 95th=%.01fms' % (
                np.mean(measurements), np.std(measurements), np.median(measurements),
                min(measurements), max(measurements), np.percentile(measurements, 95))
    return results


def main(args):
    if not os.path.exists(SCREENSHOTS_DIR):
        os.makedirs(SCREENSHOTS_DIR)
    if os.path.isfile(args.url):
        with open(args.url) as ifs:
            urls = ifs.read().splitlines()
    else:
        urls = [args.url]

    results = take_measurements(urls, args.n, args.proxyHostAndPort)
    if args.outfile is not None:
        with open(args.outfile, 'wb') as ofs:
            json.dump(results, ofs, indent=4)


if __name__ == '__main__':
    main(get_args())
