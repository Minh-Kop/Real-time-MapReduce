def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.rstrip().split('\t')

    f.close()


def main():
    data = read_file('../output/rating_commodity_reducer.txt')
    count_items_file = open('../output/count_items_reducer.txt', 'r')
    fw = open('../output/rating_usefulness_mapper.txt', 'w')
    user_count_items = []

    for line in count_items_file:
        user, count_items = line.strip().split('\t')
        count_items = count_items.strip().split(';')[0]

        user_count_items.append([user, count_items])

    for key, value in data:
        u1, u2 = key.strip().split(';')
        commodity = value.strip().split(';')[0]

        for i in user_count_items:
            if (u1 == i[0]):
                fw.writelines(f'{u1};{u2}\t{commodity};{u1};{i[1]}\n')
            if (u2 == i[0]):
                fw.writelines(f'{u1};{u2}\t{commodity};{u2};{i[1]}\n')

    count_items_file.close()
    fw.close()


# if __name__ == "__main__":
main()
