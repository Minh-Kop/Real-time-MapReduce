from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CountItem(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, item = key.strip().split(";")

        yield user, f"{item};{value}"

    def reducer(self, user, group):
        group = list(group)

        yield user, f"{len(group)}"


if __name__ == "__main__":
    CountItem().run()
