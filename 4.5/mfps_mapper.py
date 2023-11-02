import itertools


def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.rstrip().split('\t')

    f.close()


def mfps_mapper():
    data = itertools.chain(read_file('../output/rating_commodity_reducer.txt'),
                           read_file('../output/Suv_reducer.txt'),
                           read_file('../output/rating_details_reducer.txt'),
                           read_file('../output/rating_time_reducer.txt'))
    outputFile = open('../output/mfps_mapper.txt', 'w')
    appended_list = []

    # Create mapper result
    for key, value in data:
        u1, u2 = key.strip().split(';')
        
        appended_list.append([[u1, u2], value])
        if value.strip().split(';')[-1] != 'ru':
            appended_list.append([[u2, u1], value])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        outputFile.writelines(f'{i[0][0]};{i[0][1]}\t{i[1]}\n')

    # Close file
    outputFile.close()


mfps_mapper()
