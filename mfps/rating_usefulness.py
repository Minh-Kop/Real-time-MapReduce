import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import os


class rating_usefulness(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def count_items_mapper(self, _, line):
        key, _ = line.rstrip().split('\t')
        user, item = key.strip().split(';')
        yield user, item

    def count_items_reducer(self, user, items):
        length = len(list(items))
        yield user, f'{length}'

    def read_file(self, filename):
        arr = []
        with open(filename, 'r') as file:
            for line in file:
                arr.append(line.rstrip().split('\t'))
        return arr

    def rating_usefulness_mapper_init(self):
        rating_commodity_path = os.path.join(os.path.dirname(
            __file__), 'rating_commodity.txt')
        self.rating_commodity = self.read_file(rating_commodity_path)

    def rating_usefulness_mapper(self, user, items_counted):
        for key, value in self.rating_commodity:
            u1, u2 = key.strip().split(';')
            commodity = value.strip().split(';')[0]

            if (u1 == user):
                yield f'{u1};{u2}', f'{commodity};{u1};{items_counted}'
            if (u2 == user):
                yield f'{u1};{u2}', f'{commodity};{u2};{items_counted}'

    def rating_usefulness_reducer(self, key, values):
        values = list(values)
        values = [value.rstrip().split(';') for value in values]

        commodity, u1, count_items_1 = values[0]
        _, u2, count_items_2 = values[1]

        yield f'{u1};{u2}', f'{int(count_items_2) - int(commodity)};ru'
        yield f'{u2};{u1}', f'{int(count_items_1) - int(commodity)};ru'

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
