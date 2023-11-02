# from itertools import groupby
# from operator import itemgetter
# import math
# import datetime
# # data = '1;102\t5,-1'
# a = ['1,56', '2,69', '1,38', '3,89', '2,85', '4,88',
#      '4,88', '3,89']
# # result = []
# # current_char = a[0][0]  # initialize with the first character
# # subarray = []
# # for element in a:
# #     if element[0] == current_char:  # append to the current subarray
# #         subarray.append(element)
# #     else:  # start a new subarray
# #         result.append(subarray)
# #         subarray = [element]
# #         current_char = element[0]
# # result.append(subarray)  # append the last subarray
# # print(result)

# res = []
# sub = []

# for i in a:

#     for j in a:
#         if a.index(i) == a.index(j):
#             sub.append(i)
#         if (a.index(i) != a.index(j) and i[0] == j[0]):
#             sub.append(j)
#             a.pop(a.index(j))

#     res.append(sub.copy())
#     sub.clear()

# result = [tuple(sublist) for sublist in res]
# result = list(set(result))
# result = [list(sublist) for sublist in result]

# sum = 0

# for i in result:
#     if len(i) == 2:
#         print(i)
#         _, time1 = i[0].strip().split(',')
#         _, time2 = i[1].strip().split(',')

#     sum = sum + math.pow(math.e, abs(0.1*(int(time2) - int(time1))))


# print(sum)

# timestamp = 881250949
# dt = datetime.datetime.fromtimestamp(timestamp)
# print(dt)

# with open('u.txt', 'r') as infile, open('output_file.txt', 'w') as outfile:
#     # Loop through each line in the input file
#     for line in infile:
#         # Split the line by whitespace (tab or space)
#         items = line.strip().split()
#         # Reformat the items and write to the output file
#         outfile.write('%s;%s\t%s;%s\n' %
#                       (items[0], items[1], items[2], items[3]))

# array = [['86', '881250949'], ['377', '878887116'],
#          ['346', '884182806'], ['86', '883603013']]
#
# count = {}
# result = []
# for elem in array:
#     first_value = elem[0]
#     if first_value in count:
#         count[first_value] += 1
#         result.append(elem)
#     else:
#         count[first_value] = 1
#
# print(result)

inputFile = open('../input_file.txt', 'r')
users = []
for line in inputFile:
    user = line.strip().split('\t')[0].strip().split(';')[0]
    users.append(int(user))
users = set(users)

fw = open('../users.txt', 'w')

for i in users:
    fw.writelines(f'{i}\n')
