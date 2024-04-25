from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CreateCentroidsList(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value


if __name__ == "__main__":
    CreateCentroidsList().run()
