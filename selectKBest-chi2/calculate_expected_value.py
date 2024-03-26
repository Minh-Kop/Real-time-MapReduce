import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class ExpectedValue(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(ExpectedValue, self).configure_args()
        self.add_file_arg("--cProb-path", help="Path to the class probability file")

    def create_prob_list(self, filename):
        cProb = []
        with open(filename, "r") as file:
            for line in file:
                label, prob = line.strip().split("\t")
                cProb.append([label, float(prob)])
        return cProb

    def calculate_E_reducer_init(self):
        cProb_path = self.options.cProb_path
        self.cProb = self.create_prob_list(cProb_path)

    def calculate_E_mapper(self, _, line):
        user, vals = line.strip().split("\t")
        val, flag = vals.strip().split("|")
        if flag == "s":
            yield user, val

    def calculate_E_reducer(self, user, sum):
        vals = list(sum)[0]

        for i in self.cProb:
            E = float(vals) * i[1]
            yield user, f"{i[0]};{E}|e"

    def steps(self):
        return [
            MRStep(
                mapper=self.calculate_E_mapper,
                reducer_init=self.calculate_E_reducer_init,
                reducer=self.calculate_E_reducer,
            ),
        ]
