import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import os


class rating_usefulness(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def count_items_mapper(self, _, line):
        key, value = line.strip().split('\t')
        user, _ = key.strip().split(';')
        rating, _ = value.strip().split(';')
        yield user, rating

    def count_items_reducer(self, user, ratings):
        ratings = list(ratings)
        items_counted = len(ratings)
        avg_rating = 0
        for rating in ratings:
            avg_rating += int(rating)
        avg_rating /= float(items_counted)
        yield user, f'{items_counted};{avg_rating}'

    def read_file(self, filename):
        arr = []
        with open(filename, 'r') as file:
            for line in file:
                arr.append(line.strip().split('\t'))
        return arr

    def rating_usefulness_mapper_init(self):
        rating_commodity_path = os.path.join(os.path.dirname(
            __file__), 'rating_commodity.txt')
        self.rating_commodity = self.read_file(rating_commodity_path)

    def rating_usefulness_mapper(self, user, values):
        items_counted, avg_rating = values.strip().split(';')

        for key, value in self.rating_commodity:
            u1, u2 = key.strip().split(';')
            commodity = value.strip().split(';')[0]

            if (user == u1 or user == u2):
                yield f'{u1};{u2}', f'{commodity};{user};{items_counted};{avg_rating}'

    def rating_usefulness_reducer(self, key, values):
        values = list(values)
        values = [value.strip().split(';') for value in values]

        commodity, u1, items_counted_1, avg_rating_1 = values[0]
        _, u2, items_counted_2, avg_rating_2 = values[1]

        yield f'{u1};{u2}', f'{int(items_counted_2) - int(commodity)};{avg_rating_1};ru'
        yield f'{u2};{u1}', f'{int(items_counted_1) - int(commodity)};{avg_rating_2};ru'

    def steps(self):
        return [
            MRStep(mapper=self.count_items_mapper,
                   reducer=self.count_items_reducer),
            MRStep(mapper_init=self.rating_usefulness_mapper_init,
                   mapper=self.rating_usefulness_mapper, reducer=self.rating_usefulness_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    rating_usefulness().run()
