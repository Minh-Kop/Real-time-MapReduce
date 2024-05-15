from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class RatingUsefulness(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(RatingUsefulness, self).configure_args()
        self.add_file_arg("--rating-commodity-path")

    def count_items_mapper(self, _, line):
        key, _ = line.strip().split("\t")
        user, item = key.strip().split(";")
        yield user, item

    def count_items_reducer(self, user, items):
        items = list(items)
        number_of_items = len(items)
        yield user, f"{number_of_items}"

    def read_file(self, filename):
        arr = []
        with open(filename, "r") as file:
            for line in file:
                arr.append(line.strip().split("\t"))
        return arr

    def rating_usefulness_mapper_init(self):
        rating_commodity_path = self.options.rating_commodity_path
        self.rating_commodity = self.read_file(rating_commodity_path)

    def rating_usefulness_mapper(self, user, number_of_items):
        for key, value in self.rating_commodity:
            u1, u2 = key.strip().split(";")
            commodity = value.strip().split(";")[0]

            if user == u1 or user == u2:
                yield f"{u1};{u2}", f"{commodity};{user};{number_of_items}"

    def rating_usefulness_reducer(self, key, values):
        values = list(values)
        values = [value.strip().split(";") for value in values]

        commodity, u1, number_of_items_1 = values[0]
        _, u2, number_of_items_2 = values[1]

        yield f"{u1};{u2}", f"{int(number_of_items_2) - int(commodity)};ru"
        yield f"{u2};{u1}", f"{int(number_of_items_1) - int(commodity)};ru"

    def steps(self):
        return [
            MRStep(mapper=self.count_items_mapper, reducer=self.count_items_reducer),
            MRStep(
                mapper_init=self.rating_usefulness_mapper_init,
                mapper=self.rating_usefulness_mapper,
                reducer=self.rating_usefulness_reducer,
            ),
        ]


if __name__ == "__main__":
    RatingUsefulness().run()
