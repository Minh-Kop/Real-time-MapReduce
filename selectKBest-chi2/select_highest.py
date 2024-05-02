import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np

class SelectHighest(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield None, f"{user};{value}"

    def get_max_value(self, users_values):
        users_values = [line.strip().split(";") for line in users_values]
        users_values = np.array(users_values)

        values = users_values[:, 1].astype(float)

        max_idx = np.argmax(values)
        return users_values[max_idx]

    def combiner(self, _, users_values):
        user, max_value = self.get_max_value(users_values)
        yield None, f"{user};{max_value}"

    def reducer(self, _, users_values):
        user, max_value = self.get_max_value(users_values)
        yield f"{user}", f"{max_value}"