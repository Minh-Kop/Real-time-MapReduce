import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class rating_details(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def rating_details_mapper(self, _, line):
        key, value = line.strip().split('\t')
        keys = key.strip().split(';')

        if len(keys) == 1:
            yield keys[0], value
        else:
            value = value.strip().split('|')
            for index, _ in enumerate(keys):
                yield f'{keys[index]}', f'{keys[1 - index]};{value[0]}'

    def rating_details_reducer(self, user, values):
        values = list(values)
        if (len(values) != 1):
            threshold = 0

            for index, i in enumerate(values):
                i = i.strip().split(';')
                if len(i) == 1:
                    threshold = float(i[0])
                    values.pop(index)

            values = np.array(values)

            split_array = np.array([element.split(';')
                                    for element in values])
            keys = split_array[:, 0]
            unique_keys = np.unique(keys)
            arrays_by_key = {key: [] for key in unique_keys}

            for key, element in zip(keys, values):
                arrays_by_key[key].append(element)
            arrays = [np.array(arrays_by_key[key]) for key in unique_keys]

            user2 = ''
            for array in arrays:
                count = 0
                for val in array:
                    user_t, rating1, rating2 = val.strip().split(';')
                    user2 = user_t
                    rating1 = float(rating1)
                    rating2 = float(rating2)

                    if ((rating1 < threshold and rating2 < threshold) or (rating1 > threshold and rating2 > threshold)):
                        count = count+1

                yield f'{user};{user2}', f'{count};rd'

    def steps(self):
        return [
            MRStep(mapper=self.rating_details_mapper,
                   reducer=self.rating_details_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '--combinations-path', './create_combinations.txt',
        './rating_usefulness.txt',
    ]
    rating_details().run()
