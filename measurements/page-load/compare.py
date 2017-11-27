#!/usr/bin/env python
"""Compare page load time measurements"""

import argparse
import json

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from collections import OrderedDict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=str, nargs='+',
                        help='Input files outputted by client.py')
    parser.add_argument('--names', '-n', type=str, nargs='+',
                        dest='names', help='Name of first set of data')
    parser.add_argument('--outfile', '-o', type=str, dest='outfile',
                        help='Save the resulting plot to file')
    return parser.parse_args()


def load_input(filename):
    with open(filename) as ifs:
        return json.load(ifs, object_pairs_hook=OrderedDict)


def generate_plot(results, names, outfile):
    # Results are OrderedDicts

    def get_mean_sd(result):
        mean = [np.mean(result[k]) for k in result]
        sd = [np.std(result[k]) for k in result]
        return mean, sd

    urls = [x for x in results[0]]
    ind = np.arange(len(urls))
    width = 0.9 / len(results)

    fig, ax = plt.subplots()
    rects = []
    for i, result in enumerate(results):
        barColor = 'w' if i % 2 == 0 else 'gray'
        mean, sd = get_mean_sd(result)
        rects.append(ax.bar(ind + i * width, mean, width, color=barColor,
                            yerr=sd, ecolor='r'))

    ax.set_ylabel('Full page-load latency (ms)')
    ax.set_xticks(ind + width)
    ax.set_xticklabels(urls, rotation=15, fontsize='8')

    if names is None:
        names = ['Unknown %d' % x for x in xrange(len(result))]

    ax.legend([x[0] for x in rects], names)

    if outfile is None:
        plt.show()
    else:
        plt.savefig(outfile)


def main(args):
    results = []
    keySet = None
    for file in args.files:
        result = load_input(file)
        if keySet is None:
            keySet = set(result.keys())
        results.append(result)
        if keySet != set(result.keys()):
            raise Exception('Datasets are mismatched')
    if args.names and len(args.names) != len(results):
        raise Exception('Invalid number of series names')
    generate_plot(results, args.names, args.outfile)


if __name__ == '__main__':
    main(get_args())
