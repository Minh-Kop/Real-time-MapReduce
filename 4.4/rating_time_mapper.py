import itertools


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def rating_time_mapper():
    data = itertools.chain(read_file('../output/rating_commodity_reducer.txt'),
                           read_file('../output/create_time_combinations_reducer.txt'))
    outputFile = open('../output/rating_time_mapper.txt', 'w')
    appended_list = []

    # Create mapper result
    for key, value in data:
        appended_list.append([key, value])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        outputFile.writelines(f'{i[0]}\t{i[1]}\n')

    # Close file
    outputFile.close()


rating_time_mapper()
