from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


class RemoveItemsFromMatrix(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")

        value = value.split("|")
        value = [el.strip().split(";") for el in value]
        ratings = np.array(value, dtype="f")[:, 1]
        rating_str = ";".join(map(str, ratings))

        yield user, rating_str


if __name__ == "__main__":
    #     import sys
    #
    #     sys.argv[1:] = [
    #         "hadoop_output/full-matrix.txt",
    #     ]
    RemoveItemsFromMatrix.run()
