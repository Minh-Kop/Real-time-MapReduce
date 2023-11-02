import sys
from itertools import groupby
from operator import itemgetter
import math


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.rstrip().split('\t')

    f.close()


def I_mapper():
    data = read_file('./input_file.txt')
    wdata = []
    fw = open('./output/I_mapper.txt', 'w')

    for keys, _ in data:
        user, item = keys.strip().split(';')
        wdata.append({'user': int(user), 'item': item})

    wdata.sort(key=lambda x: x['user'])

    for i in wdata:
        fw.writelines(f'{i["user"]}\t{i["item"]}\n')

    fw.close()


def I_reducer():
    data = read_file('./output/I_mapper.txt')
    fw = open('./output/I_reducer.txt', 'w')

    for key, values in groupby(list(data), itemgetter(0)):
        # print(key, len(list(values)))
        length = len(list(values))
        fw.writelines(key + '\t' + str(length) + ';-1\n')

    fw.close()


def Cuv_mapper():
    data = read_file('./input_file.txt')
    fw = open('./output/Cuv_mapper.txt', 'w')
    appended_list = []

    fr = open('./input_file.txt', 'r')
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
    data = read_file('./output/Cuv_mapper.txt')
    fw = open('./output/Cuv_reducer.txt', 'w')

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
    data = read_file('./output/Cuv_reducer.txt')
    fr1 = open('./output/I_reducer.txt', 'r')
    fw = open('./output/Suv_mapper.txt', 'w')
    I_list = []

    for i in fr1:
        user, val = i.strip().split('\t')
        I, _ = val.strip().split(';')

        I_list.append([user, I])

    fr2 = open('./input_file.txt', 'r')
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
    data = read_file('./output/Suv_mapper.txt')
    fw = open('./output/Suv_reducer.txt', 'w')
    appended_list = []

    for key, value in groupby(data, itemgetter(0)):
        temp = list(value).copy()
        user1, user2 = key.strip().split(';')

        C, u1, I1 = temp[0][1].strip().split(';')
        _, u2, I2 = temp[1][1].strip().split(';')

        if u1 == user1:
            appended_list.append([int(u1), int(u2), int(I1) - int(C)])
            # fw.writelines('{};{}\t{}\n'.format(u1, u2, str(int(I1) - int(C))))

        if u2 == user2:
            appended_list.append([int(u2), int(u1), int(I2) - int(C)])
            # fw.writelines('{};{}\t{}\n'.format(u2, u1, str(int(I2) - int(C))))

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        fw.writelines('{};{}\t{};ru\n'.format(i[0], i[1], i[2]))

    fw.close()


def Tuv_mapper():
    data = read_file('./input_file.txt')
    fw = open('./output/Tuv_mapper.txt', 'w')

    file = open('./input_file.txt', 'r')
    users = []
    appended_list = []

    for i in file:
        k, _ = i.strip().split('\t')
        u, _ = k.strip().split(';')
        users.append(int(u))

    users = set(users)

    for keys, values in data:
        user, item = keys.strip().split(';')
        _, time = values.strip().split(';')

        for i in users:
            curr_user = int(user)
            sele_user = int(i)

            if (curr_user < sele_user):
                appended_list.append([[curr_user, sele_user], item, time])
            if (curr_user > sele_user):
                appended_list.append([[sele_user, curr_user], item, time])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        fw.writelines('{};{}\t{};{}\n'.format(i[0][0], i[0][1], i[1], i[2]))

    fw.close()


def Tuv_reducer():
    data = read_file('./output/Tuv_mapper.txt')
    fw = open('./output/Tuv_reducer.txt', 'w')

    appended_list = []

    for keys, values in groupby(data, itemgetter(0)):
        user1, user2 = keys.strip().split(';')

        a = list(values).copy()

        time_list = []
        for val in a:
            item, time = val[1].strip().split(';')
            time_list.append([int(item), int(time)])

        time_list = sorted(time_list, key=lambda x: x[0])

        for i, times in groupby(time_list, itemgetter(0)):
            t = list(times)
            n = len(list(t))
            if (n == 2):
                # fw.writelines('{};{};{}\t{}')
                appended_list.append([user1, user2, i, abs(t[0][1] - t[1][1])])
            elif (n == 1):
                appended_list.append([user1, user2, i, 0])

        # print(appended_list)

    t_diff = []
    for i in appended_list:
        alpha = 0.000001  # 10^-1
        e = math.e

        tuv = pow(e, -alpha*i[3])
        if i[3] != 0:
            t_diff.append([[i[0], i[1]], i[2], tuv])
        else:
            t_diff.append([[i[0], i[1]], i[2], 0])

    for users, tdiff in groupby(t_diff, itemgetter(0)):
        b = list(tdiff)

        sum = 0
        for i in b:
            sum = sum + i[2]

        fw.writelines('{};{}\t{}\n'.format(users[0], users[1], sum))

    fw.close()


I_mapper()
I_reducer()

Cuv_mapper()
Cuv_reducer()

Suv_mapper()
Suv_reducer()

Tuv_mapper()
Tuv_reducer()
