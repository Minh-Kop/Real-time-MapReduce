from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


class AvgAndSum(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(AvgAndSum, self).configure_args()
        self.add_passthru_arg("--n", type=int, help="Number of items")

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, _ = key.strip().split(";")
        rating, _ = value.strip().split(";")

        yield user, rating

    def reducer(self, user, ratings):
        number_of_items = self.options.n
        ratings = np.array(list(ratings), dtype=float)
        avg_rating = np.mean(ratings)
        sum_rating = np.sum(ratings)

        yield user, f"{(number_of_items - len(ratings)) * avg_rating + sum_rating}|s"
        yield user, f"{avg_rating}|a"


if __name__ == "__main__":
    AvgAndSum.run()
