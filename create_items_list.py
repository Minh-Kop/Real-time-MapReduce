import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class create_items_list(MRJob):
    # OUTPUT_PROTOCOL = TextProtocol

    def create_items_list_mapper(self, _, line):
        currentUser, item = (line.strip().split('\t'))[0].strip().split(';')
        item = int(item)

        yield item, '1'

    def create_items_list_reducer(self, item, _):
        # yield item, '1'
        yield '1', item

    def sort_reducer(self, _, items):
        sorted_items = sorted(list(items))
        for item in sorted_items:
            yield item, '1'
            rating['i1']=[1, 3, 4]

    def steps(self):
        return [
            MRStep(mapper=self.create_items_list_mapper,
                   reducer=self.create_items_list_reducer),
            MRStep(reducer=self.sort_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './input_file.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    create_items_list().run()
