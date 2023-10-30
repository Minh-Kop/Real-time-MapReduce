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
    data = read_input(sys.stdin)

    filename = ''  # user data file
    users = read_ids(filename)  # danh sach user = [...: int value]

    for key, values in data:
        s_user, item = key.strip().split(';')  # tach key ra thanh user & item
        user = int(s_user)

        _, time = values.strip().split(';')  # tach rating ra khoi time

        for i in users:
            if i != user:
                if (user < i):
                    print("%i;%i\t%s%s", user, i, item, time)
                elif (user > i):
                    ("%i;%i\t%s%s", i, user, item, time)


if __name__ == "__main__":
    main()
