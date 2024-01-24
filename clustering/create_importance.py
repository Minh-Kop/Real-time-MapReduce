import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class Importance(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def importance_mapper(self, _, line):
        key, value = line.strip().split('\t')
        user, _ = key.strip().split(';')
        rating, _ = value.strip().split(';')

        yield user, rating

    def importance_reducer(self, user, rating):
        rating = list(rating)
        numbers_of_rating = len(rating)
        types_of_rating = len(set(rating))
        user_importance = numbers_of_rating + types_of_rating

        yield user, f'{user_importance}'

    def steps(self):
        return [
            MRStep(mapper=self.importance_mapper,
                   reducer=self.importance_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    Importance().run()
