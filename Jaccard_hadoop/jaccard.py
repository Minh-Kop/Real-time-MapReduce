from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class Jaccard(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        key = key.strip().split(";")
        if len(key) == 1:
            yield key[0], value
        else:
            yield key[0], f"{key[1]};{value}"
            yield key[1], f"{key[0]};{value}"

    def add_intersection_reducer(self, user, group):
        group = list(group)
        item_count = ""
        for i in group:
            value = i.strip().split(";")
            if len(value) == 1:
                item_count = value[0]

        for i in group:
            value = i.strip().split(";")
            if len(value) == 2:
                user2, intersection = value[0], int(value[1])

                if int(user) < int(user2):
                    yield f"{user};{user2}", f"{intersection};{item_count}"
                else:
                    yield f"{user2};{user}", f"{intersection};{item_count}"

    def calc_jaccard(self, users, group):
        group = list(group)
        group = [i.strip().split(";") for i in group]

        yield users, f"{(int(group[0][0])*1.0)/(int(group[0][1]) + int(group[1][1]) - int(group[0][0]))}"

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.add_intersection_reducer,
            ),
            MRStep(
                reducer=self.calc_jaccard,
            ),
        ]


if __name__ == "__main__":
    Jaccard().run()
