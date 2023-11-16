import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import os


class mfps(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mfps_mapper(self, _, line):
        key, value = line.rstrip().split('\t')
        u1, u2 = key.strip().split(';')

        if value.strip().split(';')[-1] != 'ru':
            yield f'{u2};{u1}', value
        yield key, value

    def mfps_reducer(self, key, values):
        values = list(values)
        values = [value.rstrip().split(';') for value in values]
        mfps = 1
        check = True

        for line in values:
            sim = float(line[0])
            if (sim == 0):
                mfps = 0
                check = False
                break
            mfps += 1 / sim
        if check:
            mfps = 1 / mfps

        yield key, f'{mfps}'

    def steps(self):
        return [
            MRStep(mapper=self.mfps_mapper, reducer=self.mfps_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        # './rating_*.txt',  # Tệp đầu vào
        './rating_commodity.txt',  # Tệp đầu vào
        './rating_usefulness.txt',  # Tệp đầu vào
        './rating_details.txt',  # Tệp đầu vào
        './rating_time.txt',  # Tệp đầu vào
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    # mfps().run_mapper()
    mfps().run()
