import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np

class CurrentCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value

    def reducer(self, user, values):
        values = list(values)
        if(len(values) > 1):
            for i in values:
                if len(i.strip().split('|')) > 1:
                    yield user, i

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            ),
        ]

if __name__ == "__main__":
    CurrentCentroid.run()