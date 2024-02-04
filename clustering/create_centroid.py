import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class CreateCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')
        yield user, value

    def create_centroid_reducer(self, user, values):
        values = list(values)
        if (len(values) > 1):
            for value in values:
                value = value.strip()
                if (len(value.split('|')) > 1):
                    yield user, value
                    return

    def steps(self):
        return [
            MRStep(mapper=self.create_centroid_mapper,
                   reducer=self.create_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './create_user_item_matrix.txt',  # Tệp đầu vào
        './most_importance.txt',
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    CreateCentroid().run()
