
import sys


def read_input(file):
    for line in file:
        yield line.rstrip().split('\t')


def main():
    keys, _ = read_input(sys.stdin)
    user, item = keys.strip().split(';')

    print('%s\t%s', user, item)


if __name__ == '__main__':
    main()
