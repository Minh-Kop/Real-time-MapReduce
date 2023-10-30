import sys


def read_input(file):  # tach key voi value
    for line in file:
        yield line.rstrip().split('\t')


def read_ids(filename):  # danh sach user
    user_ids = []
    with open(filename, 'r') as file:
        for line in file:
            user_id = line.strip()  # Remove leading/trailing whitespaces and newlines
            user_ids.append(int(user_id))
    return user_ids


def main():
    # input comes from STDIN (standard input)
    filename = ''  # user data file
    users = read_ids(filename)  # danh sach user = [...: int value]

    # data = [key: String value, values: String value]
    data = read_input(sys.stdin)
    for key, values in data:
        s_user, _ = key.strip().split(';')  # tach key ra thanh user
        user = int(s_user)
        if values[-1] != '-1':

            value, _ = values.strip().split(';')  # tach rating ra khoi time

            for i in users:
                if i != user:
                    if (user < i):
                        print("%i;%i\t%s", user, i, value)
                    elif (user > i):
                        print("%i;%i\t%s", i, user, value)

        elif values[-1] == '-1':

            value, _ = values.strip().split(';')  # tach I ra khoi flag

            for i in users:
                if i != user:
                    if (user < i):
                        print("%i;%i\t%s;%s", user, i, value, user)
                    elif (user > i):
                        print("%i;%i\t%s;%s", i, user, value, user)


if __name__ == "__main__":
    main()
