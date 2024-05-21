from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class AvgRating(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def calculate_avg_rating_mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, _ = key.strip().split(";")
        rating, _ = value.strip().split(";")
        yield user, rating

    def calculate_avg_rating_reducer(self, user, ratings):
        ratings = list(ratings)
        items_counted = len(ratings)
        avg_rating = 0
        for rating in ratings:
            avg_rating += int(rating)
        avg_rating /= float(items_counted)
        yield user, f"{avg_rating}"

    def steps(self):
        return [
            MRStep(
                mapper=self.calculate_avg_rating_mapper,
                reducer=self.calculate_avg_rating_reducer,
            ),
        ]


if __name__ == "__main__":
    AvgRating().run()
