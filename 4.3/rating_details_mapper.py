import itertools


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.rstrip().split('\t')

    f.close()


def rating_details_mapper():
    data = itertools.chain(read_file('../Cuv_reducer.txt'),
                           read_file('./create_combinations_reducer.txt'))
    outputFile = open('./rating_details_mapper.txt', 'w')
    appended_list = []

    # Create mapper result
    for key, value in data:
        appended_list.append([key, value])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        outputFile.writelines(f'{i[0]}\t{i[1]}\n')

    # Close file
    outputFile.close()


rating_details_mapper()
