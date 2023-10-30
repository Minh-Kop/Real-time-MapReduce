
import sys


def read_mapper_output(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    keys, values = read_mapper_output(sys.stdin)

    number_of_items = len(set(values))

    print('%s\t%s;%i', keys, number_of_items, -1)  # -1: flag to differentiate
