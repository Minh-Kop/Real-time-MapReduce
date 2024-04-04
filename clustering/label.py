# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class Label(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def label_mapper(self, _, line):
        user, value = line.strip().split("\t")
        _, label = value.strip().split("&")
        yield f"{user}", f"{label}|rc"

    def steps(self):
        return [
            MRStep(mapper=self.label_mapper),
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     "./importance.txt",
    # ]
    Label().run()
