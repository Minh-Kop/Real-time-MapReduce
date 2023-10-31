import sys
from itertools import groupby
from operator import itemgetter


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.rstrip().split('\t')

    f.close()


def I_sort(e):
    return e['user']


def Suv_Sort(e):
    return e['user1_2']


def I_mapper():
    data = read_file('./output_file.txt')
    wdata = []
    fw = open('./I.txt', 'w')

    for keys, _ in data:
        user, item = keys.strip().split(';')
        wdata.append({'user': int(user), 'item': item})

    wdata.sort(key=I_sort)

    for i in wdata:
        fw.writelines(f'{i['user']}\t{i['item']}\n')

    fw.close()


def I_reducer():
    data = read_file('./I.txt')
    fw = open('./I_reducer.txt', 'w')

    for key, values in groupby(list(data), itemgetter(0)):
        # print(key, len(list(values)))
        length = len(list(values))
        fw.writelines(key + '\t' + str(length) + ';-1\n')

    fw.close()


def Cuv_mapper():
    data = read_file('./output_file.txt')
    fw = open('./Cuv.txt', 'w')
    appended_list = []

    fr = open('./output_file.txt', 'r')
    users = []

    for i in fr:
        k, _ = i.strip().split('\t')
        u, _ = k.strip().split(';')
        users.append(int(u))

    users = set(users)

    for key, _ in data:
        user, item = key.strip().split(';')
        curr = int(user)

        for i in users:
            if i < curr:
                appended_list.append([[i, curr], item])
            elif i > curr:
                appended_list.append([[curr, i], item])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        fw.writelines('{};{}\t{}\n'.format(i[0][0], i[0][1], i[1]))

    fr.close()
    fw.close()


def Cuv_reducer():
    data = read_file('./Cuv.txt')
    fw = open('./Cuv_reducer.txt', 'w')

    for key, val in groupby(data, itemgetter(0)):
        user1, user2 = key.strip().split(';')

        a = list(val).copy()
        b = []
        for i in a:
            b.append(i[1])

        b = set(b)
        # print(a)
        # print(b)
        # print('\n')

        fw.writelines('{};{}\t{}\n'.format(
            user1, user2, str(len(a) - len(b))))


I_mapper()
I_reducer()

Cuv_mapper()
Cuv_reducer()
