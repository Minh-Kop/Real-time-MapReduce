from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
from itertools import combinations


class Intersection(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        user, item = key.strip().split(";")

        yield item, f"{user};{value}"

    def create_combination_reducer(self, item, group):
        group = list(group)
        group = [i.strip().split(";") for i in group]

        comb = combinations(group, 2)
        for u, v in comb:
            if int(u[0]) < int(v[0]):
                yield f"{u[0]};{v[0]}", "1"
            else:
                yield f"{v[0]};{u[0]}", "1"

    def calc_intersection(self, users, num):
        intersection_list = list(num)

        yield users, f"{len(intersection_list)}"

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.create_combination_reducer,
            ),
            MRStep(
                reducer=self.calc_intersection,
            ),
        ]


if __name__ == "__main__":
    Intersection().run()
