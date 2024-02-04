import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class GetMax(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def get_max_mapper(self, _, line):
        user, value = line.strip().split('\t')
        yield None, f'{user};{value}'

    def get_max_reducer(self, _, values):
        values = [line.strip().split(';') for line in values]
        values = np.array(values)

        list = values[:, 1]

        index = np.argmax(list)
        max = values[index]
        max_key, max_value = max
        yield f'{max_key}', f'{max_value}'

    def steps(self):
        return [
            MRStep(mapper=self.get_max_mapper,
                   reducer=self.get_max_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './importance.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    GetMax().run()
