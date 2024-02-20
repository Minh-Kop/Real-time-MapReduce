inputFile = open('../input_file.txt', 'r')
users = []
for line in inputFile:
    user = line.strip().split('\t')[0].strip().split(';')[0]
    users.append(int(user))
users = set(users)

fw = open('../users.txt', 'w')

for i in users:
    fw.writelines(f'{i}\n')
