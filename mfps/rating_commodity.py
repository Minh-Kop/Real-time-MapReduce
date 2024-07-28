from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class RatingCommodity(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        users, value = line.strip().split("\t")
        yield users, value

    def reducer(self, users, values):
        values = list(values)
        number_of_values = len(values)
        rating_commodity = 0 if number_of_values == 1 else number_of_values - 1

        yield users, f"{rating_commodity};rc"


if __name__ == "__main__":
    RatingCommodity().run()
