import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from itertools import combinations
from math import e


class rating_time(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_time_combinations_mapper(self, _, line):
        key, value = line.rstrip().split('\t')
        user, item = key.strip().split(';')
        time = value.strip().split(';')[1]
        yield item, f'{user};{time}'

    def create_time_combinations_reducer(self, item, group):
        group = list(group)
        group = [i.rstrip().split(';') for i in group]

        comb = combinations(group, 2)
        for u, v in comb:
            if (int(u[0]) < int(v[0])):
                yield f'{u[0]};{v[0]}', f'{u[1]};{v[1]}'
            else:
                yield f'{v[0]};{u[0]}', f'{v[1]};{u[1]}'

    def rating_time_reducer(self, users, values):
        values = list(values)
        values = [value.rstrip().split(';') for value in values]

        alpha = 10**-6
        sum = 0

        for line in values:
            user1_time, user2_time = line[:2]
            user1_time = float(user1_time)
            user2_time = float(user2_time)
            sum += pow(e, -alpha * abs(user1_time - user2_time))

        yield users, f'{sum};rt'

    def steps(self):
        return [
            MRStep(mapper=self.create_time_combinations_mapper,
                   reducer=self.create_time_combinations_reducer),
            MRStep(reducer=self.rating_time_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    rating_time().run()
