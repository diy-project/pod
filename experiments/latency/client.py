#!/usr/bin/env python

import argparse
import json
import sys
import time

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from subprocess import check_output

from collections import OrderedDict

DEBUG = False

if DEBUG:
    MAX_SIZE_TO_REQUEST = 10 * 1024
    NUM_TRIALS_PER_SIZE = 3
else:
    MAX_SIZE_TO_REQUEST = 16 * 1024 * 1024
    NUM_TRIALS_PER_SIZE = 10

SECONDS_BETWEEN_REQUESTS = 0.050


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('hostAndPort', type=str)
    parser.add_argument('--no-measure', '-nm', action='store_true',
                        dest='noMeasure',
                        help='Regenerate plot with cached measurements')
    parser.add_argument('--output-prefix', type=str, default='output',
                        dest='outputPrefix',
                        help='File prefix to write the resulting plots and '
                             'measurement files')
    return parser.parse_args()


def single_measurement(hostAndPort, size, enableProxy):
    args = [
        'curl', '-o', '/dev/null', '-s',
        '-w', '%{time_total},%{speed_download}'
    ]
    if enableProxy:
        args.extend(['-x', 'localhost'])
    args.append('%s/%d' % (hostAndPort, size))
    time, rate = check_output(args).split(',')
    return float(time), float(rate)


def take_measurements(hostAndPort, enableProxy):
    print 'Fetching %s with proxy=%s' % (hostAndPort, str(enableProxy))
    power = 0
    results = OrderedDict()
    while True:
        size = 2 ** power
        if size > MAX_SIZE_TO_REQUEST:
            break

        print '  %dB:' % size,
        sys.stdout.flush()
        resultsForSize = []
        for _ in xrange(NUM_TRIALS_PER_SIZE):
            try:
                resultsForSize.append(single_measurement(hostAndPort, size,
                                                         enableProxy))
            except Exception as e:
                print >> sys.stderr, e
            time.sleep(SECONDS_BETWEEN_REQUESTS)
            print '.',
            sys.stdout.flush()
        results[size] = resultsForSize
        power += 1
        print ''
        sys.stdout.flush()
    return results


def plot_measurements(noProxy, withProxy, outputFile):
    def index_entries(tupleList, i, func=lambda x: x):
        return [func(x[i]) for x in tupleList]

    def unpack_results(results):
        meanLatency = []
        sdLatency = []
        meanRate = []
        sdRate = []
        for size in results:
            meanLatency.append(np.mean(index_entries(results[size], 0,
                                                     lambda x: x * 1000)))
            sdLatency.append(np.std(index_entries(results[size], 0,
                                                  lambda x: x * 1000)))
            meanRate.append(np.mean(index_entries(results[size], 1,
                                    lambda x: x * 8 / 1024)))
            sdRate.append(np.std(index_entries(results[size], 1,
                                               lambda x: x * 8 / 1024)))
        return meanLatency, sdLatency, meanRate, sdRate

    noProxyMeanLatency, noProxySdLatency, noProxyMeanRate, noProxySdRate = \
        unpack_results(noProxy)

    withProxyMeanLatency, withProxySdLatency, withProxyMeanRate, withProxySdRate = \
        unpack_results(withProxy)

    x = [z for z in noProxy.keys()]
    nSamples = len(noProxy[x[0]])

    fig, (ax0, ax1) = plt.subplots(nrows=2, sharex=True)

    ax0.errorbar(x, noProxyMeanLatency, yerr=noProxySdLatency, fmt='-ro',
                 label='no proxy')
    for i, j in zip(x, noProxyMeanLatency):
        ax0.annotate(str(j), xy=(i, j/2), color='red', fontsize=4)
    ax0.errorbar(x, withProxyMeanLatency, yerr=withProxySdLatency, fmt='-bx',
                 label='with proxy')
    for i, j in zip(x, withProxyMeanLatency):
        ax0.annotate(str(j), xy=(i, j*2), color='blue', fontsize=4)
    ax0.set_title('Average request latency vs. response size (N=%d)' % nSamples)
    ax0.set_xscale('log')
    ax0.set_ylabel('milliseconds')
    ax0.set_yscale('log')
    ax0.legend(loc=0)

    ax1.errorbar(x, noProxyMeanRate, yerr=noProxySdRate, fmt='-ro',
                 label='no proxy')
    ax1.errorbar(x, withProxyMeanRate, yerr=withProxySdRate, fmt='-bx',
                 label='with proxy')
    ax1.set_title('Average data rate vs. response size (N=%d)' % nSamples)
    ax1.set_xscale('log')
    ax1.set_ylabel('kbps')
    ax1.legend(loc=0)

    plt.savefig(outputFile)


def main(args):
    if args.noMeasure:
        with open(args.outputPrefix + '-no-proxy.json', 'r') as ifs:
            noProxy = json.load(ifs, object_pairs_hook=OrderedDict)
        with open(args.outputPrefix + '-with-proxy.json', 'r') as ifs:
            withProxy = json.load(ifs, object_pairs_hook=OrderedDict)
    else:
        noProxy = take_measurements(args.hostAndPort, False)
        withProxy = take_measurements(args.hostAndPort, True)

        with open(args.outputPrefix + '-no-proxy.json', 'w') as ofs:
            json.dump(noProxy, ofs, indent=4)

        with open(args.outputPrefix + '-with-proxy.json', 'w') as ofs:
            json.dump(withProxy, ofs, indent=4)

    plot_measurements(noProxy, withProxy, args.outputPrefix + '.pdf')


if __name__ == '__main__':
    main(get_args())
