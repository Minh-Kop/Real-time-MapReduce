from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class Label(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        _, label = value.strip().split("&")
        yield user, label


if __name__ == "__main__":
    Label().run()
