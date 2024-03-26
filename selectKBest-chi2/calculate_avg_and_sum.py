import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class AvgAndSum(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(AvgAndSum, self).configure_args()
        self.add_passthru_arg("--n", help="Number of items")

    def calculate_avg_and_sum_rating_mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, _ = key.strip().split(";")
        rating, _ = value.strip().split(";")

        yield user, rating

    def calculate_avg_and_sum_rating_reducer(self, user, rating):
        nItems = int(self.options.n)
        vals = np.array(list(rating), dtype=float)
        uAvg = np.mean(vals)
        uSum = np.sum(vals)

        yield user, f"{(nItems - len(vals))*uAvg + uSum}|s"
        yield user, f"{uAvg}|a"

    def steps(self):
        return [
            MRStep(
                mapper=self.calculate_avg_and_sum_rating_mapper,
                reducer=self.calculate_avg_and_sum_rating_reducer,
            ),
        ]
