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
        fw.writelines(f'{i["user"]}\t{i["item"]}\n')

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

    fw.close()


def Suv_mapper():
    data = read_file('./Cuv_reducer.txt')
    fr1 = open('./I_reducer.txt', 'r')
    fw = open('./Suv.txt', 'w')
    I_list = []

    for i in fr1:
        user, val = i.strip().split('\t')
        I, _ = val.strip().split(';')

        I_list.append([user, I])

    fr2 = open('./output_file.txt', 'r')
    users = []

    for i in fr2:
        k, _ = i.strip().split('\t')
        u, _ = k.strip().split(';')
        users.append(int(u))

    users = set(users)

    for user, C in data:
        u1, u2 = user.strip().split(';')

        for i in I_list:
            if (u1 == i[0]):
                fw.writelines('{};{}\t{};{};{}\n'.format(u1, u2, C, u1, i[1]))
            if (u2 == i[0]):
                fw.writelines('{};{}\t{};{};{}\n'.format(u1, u2, C, u2, i[1]))

    fr1.close()
    fr2.close()
    fw.close()


def Suv_reducer():
    data = read_file('./Suv.txt')
    fw = open('./Suv_reducer.txt', 'w')
    appended_list = []

    for key, value in groupby(data, itemgetter(0)):
        temp = list(value).copy()
        user1, user2 = key.strip().split(';')

        C, u1, I1 = temp[0][1].strip().split(';')
        _, u2, I2 = temp[1][1].strip().split(';')

        if u1 == user1:
            appended_list.append([u1, u2, str(int(I1) - int(C))])
            # fw.writelines('{};{}\t{}\n'.format(u1, u2, str(int(I1) - int(C))))

        if u2 == user2:
            appended_list.append([u2, u1, str(int(I2) - int(C))])
            # fw.writelines('{};{}\t{}\n'.format(u2, u1, str(int(I2) - int(C))))

    appended_list = sorted(appended_list, key=lambda x: x[0], reverse=True)

    for i in appended_list:
        fw.writelines('{};{}\t{}\n'.format(i[0], i[1], i[2]))


I_mapper()
I_reducer()

Cuv_mapper()
Cuv_reducer()

Suv_mapper()
Suv_reducer()
