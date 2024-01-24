import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class MostImportance(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def most_importance_mapper(self, _, line):
        user, importance = line.strip().split('\t')
        yield 'max', f'{user};{importance}'

    def most_importance_reducer(self, _, user_importance):
        user_importance = [line.strip().split(';') for line in user_importance]
        user_importance = np.array(user_importance, dtype='i')

        importance_list = user_importance[:, 1]

        max_importance_index = np.argmax(importance_list)
        most_importance = user_importance[max_importance_index]
        user, importance = most_importance
        yield f'{user}', f'{importance}'

    def steps(self):
        return [
            MRStep(mapper=self.most_importance_mapper,
                   reducer=self.most_importance_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './importance.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    MostImportance().run()
