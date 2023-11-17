from itertools import groupby
from operator import itemgetter


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def rating_commodity_reducer():
    data = read_file('../output/rating_commodity_mapper.txt')
    fw = open('../output/rating_commodity_reducer.txt', 'w')

    for key, val in groupby(data, itemgetter(0)):
        user1, user2 = key.strip().split(';')

        a = list(val).copy()

        b = []
        for i in a:
            b.append(i[1])
        b = set(b)

        fw.writelines(f'{user1};{user2}\t{str(len(a) - len(b))};rc\n')

    fw.close()


rating_commodity_reducer()
