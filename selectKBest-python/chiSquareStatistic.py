from sklearn.datasets import load_digits
from sklearn.feature_selection import SelectKBest, chi2
from sklearn.preprocessing import LabelBinarizer
import numpy as np
from sklearn.datasets import load_iris
import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


def run_mr_job(mr_job_class, input_args):
    mr_job = mr_job_class(args=input_args)
    with mr_job.make_runner() as runner:
        runner.run()
        data = []
        for key, value in mr_job.parse_output(runner.cat_output()):
            data.append(f"{key}\t{value}")
        return data


def write_data_to_file(filename, data, mode="w"):
    output_file = open(filename, mode)
    for el in data:
        output_file.writelines(el)
    output_file.close()


class ItemUserMatrix(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_item_user_matrix_mapper(self, _, line):
        user, value = line.strip().split("\t")
        values = value.strip().split("|")
        for i in values:
            item, rating = i.strip().split(";")
            yield item, f"{user};{rating}"

    def create_item_user_matrix_reducer(self, item, values):
        values = list(values)
        ratings = []
        for val in values:
            user, rate = val.strip().split(";")
            ratings.append(f"{rate}")
        ratings = ";".join(ratings)

        yield item, ratings

    def steps(self):
        return [
            MRStep(
                mapper=self.create_item_user_matrix_mapper,
                reducer=self.create_item_user_matrix_reducer,
            ),
        ]


result_data = run_mr_job(ItemUserMatrix, ["./input/user_item_matrix copy.txt"])
write_data_to_file("./selectKBest-python/item_user_matrix copy.txt", result_data)

bTable = [
    3,
    4,
    1,
    3,
    5,
    1,
    2,
]
X = []


with open("./selectKBest-python/item_user_matrix copy.txt", "r") as file:
    for line in file:
        item, user_rating = line.strip().split("\t")
        user_rating = user_rating.strip().split(";")

        X.append([float(rating) for rating in user_rating])

print(X)

X_new = SelectKBest(chi2, k="all")
X_new.fit_transform(X, bTable)

chi2_scores = X_new.scores_

print("Chi2 scores for each feature:")
for score in chi2_scores:
    print(f"{score}")
