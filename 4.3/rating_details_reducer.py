from itertools import groupby
from operator import itemgetter


def read_mapper_output(file, separator='\t'):
    file = open(file, 'r')
    for line in file:
        yield line.strip().split(separator)
    file.close()


def rating_details_reducer():
    data = read_mapper_output(
        '../output/rating_details_mapper.txt', '\t')
    outputFile = open('../output/rating_details_reducer.txt', 'w')

    # Create reducer result
    for u_v, group in groupby(data, itemgetter(0)):
        try:
            group = list(group)
            group = [i[1].strip().split(';') for i in group]
            count = 0
            liking_threshold = 3.5

            for line in group:
                if (line[-1] == 'rc'):
                    continue
                r_ui, r_vi = line[:2]
                r_ui = float(r_ui)
                r_vi = float(r_vi)
                if (r_ui > liking_threshold and r_vi > liking_threshold):
                    count += 1
                elif (r_ui < liking_threshold and r_vi < liking_threshold):
                    count += 1

            outputFile.writelines(f'{u_v}\t{count};rd\n')
        except ValueError:
            pass

    # Close file
    outputFile.close()


rating_details_reducer()
