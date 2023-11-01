from itertools import groupby, combinations
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.rstrip().split(separator)
    file.close()


def create_combinations_reducer():
    data = read_mapper_output(
        './create_combinations_mapper.txt', '\t')
    outputFile = open('./create_combinations_reducer.txt', 'w')

    # Create reducer result
    for current_item, group in groupby(data, itemgetter(0)):
        try:
            group = list(group)
            print(group)
            group = [i[1].rstrip().split(';') for i in group]
            comb = combinations(group, 2)
            for u, v in comb:
                if (int(u[0]) < int(v[0])):
                    outputFile.writelines(f'{u[0]};{v[0]}\t{u[1]};{v[1]};-1\n')
                else:
                    outputFile.writelines(f'{v[0]};{u[0]}\t{v[1]};{u[1]};-1\n')
        except ValueError:
            pass

    # Close file
    outputFile.close()


create_combinations_reducer()
