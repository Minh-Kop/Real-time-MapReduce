from itertools import groupby, combinations
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.strip().split(separator)
    file.close()


def create_time_combinations_reducer():
    data = read_mapper_output(
        '../output/create_time_combinations_mapper.txt', '\t')
    outputFile = open('../output/create_time_combinations_reducer.txt', 'w')

    # Create reducer result
    for current_item, group in groupby(data, itemgetter(0)):
        try:
            group = list(group)
            group = [i[1].strip().split(';') for i in group]
            comb = combinations(group, 2)
            for u, v in comb:
                if (int(u[0]) < int(v[0])):
                    outputFile.writelines(f'{u[0]};{v[0]}\t{u[1]};{v[1]}\n')
                else:
                    outputFile.writelines(f'{v[0]};{u[0]}\t{v[1]};{u[1]}\n')
        except ValueError:
            pass

    # Close file
    outputFile.close()


create_time_combinations_reducer()
