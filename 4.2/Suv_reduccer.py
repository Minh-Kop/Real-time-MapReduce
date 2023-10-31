import sys
from itertools import groupby
from operator import itemgetter


def read_mapper_output(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    # data = [key: string(num1;num2) , value[item1,item2,...: string]]
    data = read_mapper_output(sys.stdin)
    for key, values in groupby(data, itemgetter(0)):
        user1, user2 = key.split(';')

        I1 = 0
        I2 = 0
        val_copy = values.copy

        for i in values:
            temp = i.split(';')
            length = len(temp)

            if length == 2:
                val_copy.pop(val_copy.index(i))

                if temp[1] == user1:
                    I1 = int(temp[0])
                elif temp[1] == user2:
                    I2 = int(temp[0])

        same_item = len(val_copy) - len(set(val_copy))
        print('%s;%s\t%i', user1, user2, I2 - same_item)


if __name__ == "__main__":
    main()
