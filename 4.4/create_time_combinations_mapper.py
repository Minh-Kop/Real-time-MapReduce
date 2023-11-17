def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def create_time_combinations_mapper():
    data = read_file('../input_file.txt')
    outputFile = open('../output/create_time_combinations_mapper.txt', 'w')
    appended_list = []

    # Create mapper result
    for key, value in data:
        user, item = key.strip().split(';')
        currentUser = int(user)
        time = value.strip().split(';')[1]

        appended_list.append([item, currentUser, time])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        outputFile.writelines(f'{i[0]}\t{i[1]};{i[2]}\n')

    # Close file
    outputFile.close()


create_time_combinations_mapper()
