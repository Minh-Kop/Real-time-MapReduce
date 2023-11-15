from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.rstrip().split(separator)
    file.close()


def main():
    data = read_mapper_output('../output/rating_usefulness_mapper.txt', '\t')
    fw = open('../output/rating_usefulness_reducer.txt', 'w')
    appended_list = []

    for key, group in groupby(data, itemgetter(0)):
        group = list(group)
        group = [i[1].rstrip().split(';') for i in group]

        commodity, u1, count_items_1 = group[0]
        _, u2, count_items_2 = group[1]

        appended_list.append(
            [int(u1), int(u2), int(count_items_1) - int(commodity)])
        # fw.writelines('{};{}\t{}\n'.format(u1, u2, str(int(count_items_1) - int(commodity))))

        appended_list.append(
            [int(u2), int(u1), int(count_items_2) - int(commodity)])
        # fw.writelines('{};{}\t{}\n'.format(u2, u1, str(int(count_items_2) - int(commodity))))

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        fw.writelines(f'{i[0]};{i[1]}\t{i[2]};ru\n')

    fw.close()


# if __name__ == "__main__":
main()
