import sys
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
                yield item, f"{user};{rating}"

    def add_label_to_user_reducer(self, item, values):
        values = list(values)
        for i in values:
            if len(i.strip().split("|")) > 1:
                label = i

        for user_rating in values:
            if len(user_rating.strip().split("|")) == 1:
                user, rating = user_rating.split(";")
                if user == "166":
                    a = 0
                yield f"{label};{user}", rating

    def calculate_O_mapper(self, label_user, rating):
        yield label_user, rating

    def calculate_O_reducer(self, label_user, rating):
        label, user = label_user.strip().split(";")
        new_rate = np.array(list(rating))
        if user == "166":
            a = 0
        total_sum = np.sum(new_rate, dtype=float)
        yield user, f"{label};{total_sum}|o"

    def steps(self):
        return [
            MRStep(
                mapper=self.add_label_to_user_mapper,
                reducer=self.add_label_to_user_reducer,
            ),
            MRStep(
                mapper=self.calculate_O_mapper,
                reducer=self.calculate_O_reducer,
            ),
        ]
