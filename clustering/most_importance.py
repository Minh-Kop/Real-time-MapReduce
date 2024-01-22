import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class MostImportance(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def most_importance_mapper(self, _, line):
        user, importance = line.strip().split('\t')

        yield 'max', f'{user};{importance}'

    def most_importance_reducer(self, _, user_importance):
        # most_importance_user = None
        # max_importance = -1

        # for i in user_importance:
        #     user, importance = i.strip().split(';')
        #     importance = int(importance)
        #     if importance > max_importance:
        #         most_importance_user = user
        #         max_importance = importance

        # yield most_importance_user, str(max_importance)
        # a, b = user_importance.strip().split(';')
        yield None, None

    def steps(self):
        return [
            MRStep(mapper=self.most_importance_mapper,
                   reducer=self.most_importance_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './create_combinations.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    MostImportance().run()

# f1 = open('./create_combinations.txt')
# print('output:\n')
# print(f1.read())

# f2 = open('./create_combinations1.txt')
# print('output:\n')
# print(f2.read())
