# Create user set include unique user values
inputFile = open('./../output_file.txt', 'r')
users = []
for line in inputFile:
    user = line.strip().split('\t')[0].strip().split(';')[0]
    users.append(int(user))
users = set(users)
