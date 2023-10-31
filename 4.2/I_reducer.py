import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    data = read_mapper_output(sys.stdin)

    number_of_items = len(set(values))

    for keys, values in groupby(data, itemgetter(0)):
        print('%s\t%s;%i', keys, number_of_items, -1)  # -1: flag to differentiate
