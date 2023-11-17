# def read_input(file):
#     for line in file:
#         yield line.strip().split('\t')


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def main():
    data = read_file('../input_file.txt')
    fw = open('../output/count_items_mapper.txt', 'w')
    output = []

    for keys, _ in data:
        user, item = keys.strip().split(';')
        output.append({'user': int(user), 'item': item})

    output.sort(key=lambda x: x['user'])

    for i in output:
        fw.writelines(f'{i["user"]}\t{i["item"]}\n')

    fw.close()


# if __name__ == '__main__':
#     main()
main()
