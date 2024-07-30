from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CreateCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value

    def reducer(self, user, values):
        values = list(values)
        if len(values) > 1:
            for value in values:
                value = value.strip()
                if len(value.split(";")) > 1:
                    yield user, value
                    return


if __name__ == "__main__":
    CreateCentroids().run()
