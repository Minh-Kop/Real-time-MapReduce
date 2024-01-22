import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import os


class calculate_avg_rating(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def calculate_avg_rating_mapper(self, _, line):
        key, value = line.strip().split('\t')
        user, _ = key.strip().split(';')
        rating, _ = value.strip().split(';')
        yield user, rating

    def calculate_avg_rating_reducer(self, user, ratings):
        ratings = list(ratings)
        items_counted = len(ratings)
        avg_rating = 0
        for rating in ratings:
            avg_rating += int(rating)
        avg_rating /= float(items_counted)
        yield user, f'{avg_rating}'

    def steps(self):
        return [
            MRStep(mapper=self.calculate_avg_rating_mapper,
                   reducer=self.calculate_avg_rating_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    calculate_avg_rating().run()
