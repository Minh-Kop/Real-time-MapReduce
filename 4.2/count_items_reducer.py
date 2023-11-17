from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.strip().split(separator)
    file.close()


def main():
    data = read_mapper_output('../output/count_items_mapper.txt', '\t')
    fw = open('../output/count_items_reducer.txt', 'w')

    for key, values in groupby(list(data), itemgetter(0)):
        length = len(list(values))
        fw.writelines(f'{key}\t{length};-1\n')

    fw.close()


main()
