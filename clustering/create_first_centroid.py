import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class CreateFirstCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def first_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')
        yield user, value

    def first_centroid_reducer(self, user, values):
        values = list(values)
        if (len(values) > 1):
            for value in values:
                temp_value = value.strip().split(';')
                if (len(temp_value) > 1):
                    yield user, value

    def steps(self):
        return [
            MRStep(mapper=self.first_centroid_mapper,
                   reducer=self.first_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './create_user_item_matrix.txt',  # Tệp đầu vào
        './most_importance.txt',
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    CreateFirstCentroid().run()
