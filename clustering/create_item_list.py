import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class create_item_list(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_item_list_mapper(self, _, line):
        item = line.strip().split('\t')[0].strip().split(';')[1]
        yield 1, item

    def create_item_list_reducer(self, _, items):
        items = set(items)

        for i in items:
            yield i, ''

    def steps(self):
        return [
            MRStep(mapper=self.create_item_list_mapper,
                   reducer=self.create_item_list_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    create_item_list().run()
