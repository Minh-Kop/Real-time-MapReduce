from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class Jaccard(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def preparation_mapper(self, _, line):
        key, value = line.strip().split("\t")
        key = key.strip().split(";")
        if len(key) == 1:
            yield key[0], value
        else:
            yield key[0], f"{key[1]};{value}"
            yield key[1], f"{key[0]};{value}"

    def preparation_reducer(self, user, values):
        values = list(values)
        number_of_items = 0

        # Loop through values to get number of items of current user
        for i in values:
            value = i.strip().split(";")
            if len(value) == 1:
                number_of_items = value[0]
                break

        for i in values:
            value = i.strip().split(";")
            if len(value) == 2:
                user2, number_of_intersected_items = value[0], int(value[1])

                if int(user) < int(user2):
                    yield f"{user};{user2}", f"{number_of_intersected_items};{number_of_items}"
                else:
                    yield f"{user2};{user}", f"{number_of_intersected_items};{number_of_items}"

    def calc_jaccard_reducer(self, users, values):
        values = list(values)
        values = [i.strip().split(";") for i in values]

        number_of_intersected_items = float(values[0][0])
        number_of_items_1 = int(values[0][1])
        number_of_items_2 = int(values[1][1])

        jaccard_simp = number_of_intersected_items / (
            number_of_items_1 + number_of_items_2 - number_of_intersected_items
        )

        user1, user2 = users.strip().split(";")
        yield f"{user1};{user2}", str(jaccard_simp)
        yield f"{user2};{user1}", str(jaccard_simp)

    def steps(self):
        return [
            MRStep(
                mapper=self.preparation_mapper,
                reducer=self.preparation_reducer,
            ),
            MRStep(
                reducer=self.calc_jaccard_reducer,
            ),
        ]


if __name__ == "__main__":
    Jaccard().run()
