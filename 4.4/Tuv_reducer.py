import sys
import math


def read_mapper_output(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    # data = [key: string(num1;num2) , value[item1,item2,...: string]]
    key, values = read_mapper_output(sys.stdin)
    user1, user2 = key.strip().split(';')

    res = []
    sub = []

    for i in values:

        for j in values:
            if i == j:
                sub.append(i)
            if (i != j and i[0] == j[0]):
                sub.append(j)
                values.pop(values.index(j))

        res.append(sub.copy())
        sub.clear()

    result = [tuple(sublist) for sublist in res]
    result = list(set(result))
    result = [list(sublist) for sublist in result]

    sun = 0

    for i in result:
        if len(i) == 2:
            _, time1 = i[0].strip().split(';')
            _, time2 = i[1].strip().split(';')

        sum = sum + math.pow(math.e, abs(0.1*(int(time2) - int(time1))))

    print('%s;%s\t%i', user1, user2, sum)
