from itertools import combinations

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class CreateCombinations(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, item = key.strip().split(";")
        rating, time = value.strip().split(";")
        yield item, f"{user};{rating};{time}"

    def reducer(self, item, group):
        group = list(group)
        group = [i.strip().split(";") for i in group]

        comb = combinations(group, 2)
        for u, v in comb:
            if int(u[0]) < int(v[0]):
                yield f"{u[0]};{v[0]}", f"{u[1]};{v[1]}|{u[2]};{v[2]}"
            else:
                yield f"{v[0]};{u[0]}", f"{v[1]};{u[1]}|{v[2]};{u[2]}"


if __name__ == "__main__":
    CreateCombinations().run()
