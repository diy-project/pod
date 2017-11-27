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
    parser.add_argument('file1', type=str,
                        help='First input file outputted by client.py')
    parser.add_argument('file2', type=str,
                        help='Second input file outputted by client.py')
    parser.add_argument('--name1', '-n1', type=str, default='one',
                        dest='name1', help='Name of first set of data')
    parser.add_argument('--name2', '-n2', type=str, default='two',
                        dest='name2', help='Name of second set of data')
    parser.add_argument('--outfile', '-o', type=str, dest='outfile',
                        help='Save the resulting plot to file')
    return parser.parse_args()


def load_input(filename):
    with open(filename) as ifs:
        return json.load(ifs, object_pairs_hook=OrderedDict)


def generate_plot(result1, name1, result2, name2, outfile):
    # Results are OrderedDicts

    def get_mean_sd(result):
        mean = [np.mean(result[k]) for k in result]
        sd = [np.mean(result[k]) for k in result]
        return mean, sd

    names = [k for k in result1]
    result1Means, result1Sds = get_mean_sd(result1)
    result2Means, result2Sds = get_mean_sd(result2)

    ind = np.arange(len(names))
    width = 0.35

    fig, ax = plt.subplots()
    rects1 = ax.bar(ind, result1Means, width, color='w', yerr=result1Sds, ecolor='r')
    rects2 = ax.bar(ind + width, result2Means, width, color='b', yerr=result2Sds, ecolor='r')

    ax.set_ylabel('Full page-load latency (ms)')
    ax.set_xticks(ind + width)
    ax.set_xticklabels(names, rotation=15, fontsize='8')
    ax.legend((rects1[0], rects2[0]), (name1, name2))

    if outfile is None:
        plt.show()
    else:
        plt.savefig(outfile)


def main(args):
    result1 = load_input(args.file1)
    result2 = load_input(args.file2)
    if set(result1.keys()) != set(result2.keys()):
        raise Exception('Datasets are mismatched')
    generate_plot(result1, args.name1, result2, args.name2, args.outfile)


if __name__ == '__main__':
    main(get_args())
