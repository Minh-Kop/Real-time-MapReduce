from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class ObservedValue(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def add_label_to_user_mapper(self, _, line):
        if len(line.strip().split(";")) == 1:
            item, label = line.strip().split("\t")
            yield item, label
        else:
            user, values = line.strip().split("\t")
            values = values.strip().split("|")
            for val in values:
                item, rating = val.strip().split(";")
                yield str(int(float(item))), f"{user};{rating}"

    def add_label_to_user_reducer(self, item, values):
        values = list(values)
        for i in values:
            if len(i.strip().split("|")) > 1:
                label = i
                values.remove(i)
                break

        for user_rating in values:
            user, rating = user_rating.split(";")
            yield f"{user};{label}", rating

    def calculate_observed_value_reducer(self, label_user, ratings):
        user, label = label_user.strip().split(";")
        ratings = np.array(list(ratings), dtype=float)

        sum_ratings = np.sum(ratings)
        yield f"{user};{label}", f"{sum_ratings}|o"

    def steps(self):
        return [
            MRStep(
                mapper=self.add_label_to_user_mapper,
                reducer=self.add_label_to_user_reducer,
            ),
            MRStep(
                reducer=self.calculate_observed_value_reducer,
            ),
        ]


if __name__ == "__main__":
    ObservedValue.run()
