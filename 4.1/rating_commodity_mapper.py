def read_file(file):
    f = open(file, 'r')
    for line in f:
        yield line.strip().split('\t')

    f.close()


def read_ids(filename):
    user_ids = []
    with open(filename, 'r') as file:
        for line in file:
            user_id = line.strip()  # Remove leading/trailing whitespaces and newlines
            user_ids.append(int(user_id))
    return user_ids


def rating_commodity_mapper():
    data = read_file('../input_file.txt')
    fw = open('../output/rating_commodity_mapper.txt', 'w')
    users = read_ids('../users.txt')
    appended_list = []

    for key, _ in data:
        currentUser, item = key.strip().split(';')
        currentUser = int(currentUser)

        for user in users:
            if user < currentUser:
                appended_list.append([[user, currentUser], item])
            elif user > currentUser:
                appended_list.append([[currentUser, user], item])

    appended_list = sorted(appended_list, key=lambda x: x[0])

    for i in appended_list:
        fw.writelines('{};{}\t{}\n'.format(i[0][0], i[0][1], i[1]))

    fw.close()


rating_commodity_mapper()
