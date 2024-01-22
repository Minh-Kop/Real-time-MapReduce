users_file = open('../users.txt', 'r')
for number_of_users, line in enumerate(users_file, start=1):
    pass
users_file.close()

M = int(number_of_users/4/1.5)

output_file = open('calculate_number_of_discard_points.txt', 'w')
output_file.writelines(f'{M}')
output_file.close()
