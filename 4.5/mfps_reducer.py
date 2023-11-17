from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.strip().split(separator)
    file.close()


def rating_details_reducer():
    data = read_mapper_output(
        '../output/mfps_mapper.txt', '\t')
    outputFile = open('../output/mfps_reducer.txt', 'w')

    # Create reducer result
    for u_v, group in groupby(data, itemgetter(0)):
        try:
            group = list(group)
            group = [i[1].strip().split(';') for i in group]
            mfps = 1
            check = True

            for line in group:
                sim = float(line[0])
                if (sim == 0):
                    mfps = 0
                    check = False
                    break
                mfps += 1 / sim
            if check:
                mfps = 1 / mfps

            outputFile.writelines(f'{u_v}\t{mfps}\n')
        except ValueError:
            pass

    # Close file
    outputFile.close()


rating_details_reducer()
