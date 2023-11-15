import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from itertools import combinations


class rating_details(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_rating_combinations_mapper(self, _, line):
        key, value = line.rstrip().split('\t')
        user, item = key.strip().split(';')
        rating = value.strip().split(';')[0]
        yield item, f'{user};{rating}'

    def create_rating_combinations_reducer(self, item, group):
        group = list(group)
        group = [i.rstrip().split(';') for i in group]
        
        comb = combinations(group, 2)
        for u, v in comb:
            if (int(u[0]) < int(v[0])):
                yield f'{u[0]};{v[0]}', f'{u[1]};{v[1]}'
            else:
                yield f'{v[0]};{u[0]}', f'{v[1]};{u[1]}'

    def rating_details_reducer(self, users, values):
        values = list(values)
        values = [value.rstrip().split(';') for value in values]

        count = 0
        liking_threshold = 3.5

        for line in values:
            user1_rating, user2_rating = line[:2]
            user1_rating = float(user1_rating)
            user2_rating = float(user2_rating)
            if (user1_rating > liking_threshold and user2_rating > liking_threshold):
                count += 1
            elif (user1_rating < liking_threshold and user2_rating < liking_threshold):
                count += 1

        yield users, f'{count};rd'

    def steps(self):
        return [
            MRStep(
                mapper=self.create_rating_combinations_mapper, reducer=self.create_rating_combinations_reducer),
            MRStep(reducer=self.rating_details_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    rating_details().run()
