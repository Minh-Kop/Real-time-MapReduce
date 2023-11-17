def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def create_rating_combinations_mapper():
    data = read_file('../input_file.txt')
    outputFile = open('../output/create_rating_combinations_mapper.txt', 'w')
    appended_list = []

    # Create mapper result
    for key, value in data:
        user, item = key.strip().split(';')
        currentUser = int(user)
        rating = value.strip().split(';')[0]

        appended_list.append([item, currentUser, rating])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        outputFile.writelines(f'{i[0]}\t{i[1]};{i[2]}\n')

    # Close file
    outputFile.close()


create_rating_combinations_mapper()
