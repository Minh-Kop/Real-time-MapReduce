import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class ClassProbability(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(ClassProbability, self).configure_args()
        self.add_passthru_arg("--n", type=int, help="Number of items")

    def calculate_class_probability_mapper(self, _, line):
        item, label = line.strip().split("\t")
        yield label, item

    def calculate_class_probability_reducer(self, label, values):
        nItems = self.options.n
        items = len(list(values))

        yield label, f"{float(items) / nItems}"

    def steps(self):
        return [
            MRStep(
                mapper=self.calculate_class_probability_mapper,
                reducer=self.calculate_class_probability_reducer,
            ),
        ]
