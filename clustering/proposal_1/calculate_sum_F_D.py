# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class SumFD(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def sum_f_d_mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value

    def sum_f_d_reducer(self, user, values):
        values = [value.strip().split(";") for value in values]
        values = np.array(values)
        values = values.astype("f")
        f_d = np.sum(values)

        yield user, str(f_d)

    def steps(self):
        return [
            MRStep(mapper=self.sum_f_d_mapper, reducer=self.sum_f_d_reducer),
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     '../input_file.txt',
    # ]
    SumFD().run()
