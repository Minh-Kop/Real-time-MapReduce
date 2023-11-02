from itertools import groupby
from operator import itemgetter
from math import e


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.rstrip().split(separator)
    file.close()


def rating_time_reducer():
    data = read_mapper_output(
        '../output/rating_time_mapper.txt', '\t')
    outputFile = open('../output/rating_time_reducer.txt', 'w')

    # Create reducer result
    for u_v, group in groupby(data, itemgetter(0)):
        try:
            group = list(group)
            group = [i[1].rstrip().split(';') for i in group]
            alpha = 10**-6
            sum = 0

            for line in group:
                if (line[-1] == 'rc'):
                    continue
                t_ui, t_vi = line
                t_ui = float(t_ui)
                t_vi = float(t_vi)
                sum += pow(e, -alpha * abs(t_ui - t_vi))

            outputFile.writelines(f'{u_v}\t{sum};rt\n')
        except ValueError:
            pass

    # Close file
    outputFile.close()


rating_time_reducer()
