#!/usr/bin/env python

import argparse
import json
import time

import numpy as np
import matplotlib.pyplot as plt

from subprocess import check_output

from collections import OrderedDict


NUM_TRIALS_PER_SIZE = 10
SECONDS_BETWEEN_REQUESTS = 0.250

MAX_SIZE_TO_REQUEST = 64 * 1024 * 1024
MAX_SIZE_TO_REQUEST = 1 * 1024 * 1024


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('hostAndPort', type=str)
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
    power = 0
    results = OrderedDict()
    while True:
        size = 2 ** power
        if size > MAX_SIZE_TO_REQUEST:
            break
        resultsForSize = []
        for _ in xrange(NUM_TRIALS_PER_SIZE):
            try:
                resultsForSize.append(single_measurement(hostAndPort, size,
                                                         enableProxy))
            except Exception, e:
                print e
            time.sleep(SECONDS_BETWEEN_REQUESTS)
        results[size] = resultsForSize
        power += 1
    return results


def plot_measurements(noProxy, withProxy, outputFile):
    def index_entries(tupleList, i):
        return [x[i] for x in tupleList]

    def unpack_results(results):
        meanLatency = []
        sdLatency = []
        meanRate = []
        sdRate = []
        for size in results:
            meanLatency.append(np.mean(index_entries(results[size], 0)))
            sdLatency.append(np.std(index_entries(results[size], 0)))
            meanRate.append(np.mean(index_entries(results[size], 1)))
            sdRate.append(np.std(index_entries(results[size], 1)))
        return meanLatency, sdLatency, meanRate, sdRate

    noProxyMeanLatency, noProxySdLatency, noProxyMeanRate, noProxySdRate = \
        unpack_results(noProxy)

    withProxyMeanLatency, withProxySdLatency, withProxyMeanRate, withProxySdRate = \
        unpack_results(withProxy)

    x = [z for z in noProxy.keys()]
    nSamples = len(noProxy[x[0]])

    fig, (ax0, ax1) = plt.subplots(nrows=2, sharex=True)

    ax0.errorbar(x, noProxyMeanLatency, yerr=noProxySdLatency, fmt='-o')
    ax0.errorbar(x, withProxyMeanLatency, yerr=withProxySdLatency, fmt='-x')
    ax0.set_title('Average request Latency vs. response size (N=%d)' % nSamples)

    ax1.errorbar(x, noProxyMeanRate, yerr=noProxySdRate, fmt='-o')
    ax1.errorbar(x, noProxyMeanRate, yerr=noProxySdRate, fmt='-o')
    ax1.set_title('Average data rate vs. response size (N=%d)' % nSamples)

    plt.savefig(outputFile)


def main(args):
    noProxy = take_measurements(args.hostAndPort, False)
    # withProxy = take_measurements(args.hostAndPort, True)
    withProxy = noProxy

    plot_measurements(noProxy, withProxy, args.outputPrefx + '.pdf')

    with open(args.outputPrefx + '-no-proxy.json', 'w') as ofs:
        ofs.write(json.dumps(noProxy, indent=4))
        ofs.write('\n')

    with open(args.outputPrefx + '-with-proxy.json', 'w') as ofs:
        ofs.write(json.dumps(noProxy, indent=4))
        ofs.write('\n')

if __name__ == '__main__':
    main(get_args())