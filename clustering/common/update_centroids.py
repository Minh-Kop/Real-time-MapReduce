from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


class UpdateCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        _, value = line.strip().split("\t")
        ratings, centroid_id = value.split("&")
        yield centroid_id, ratings

    def reducer(self, centroid_id, ratings_list):
        ratings_list = list(ratings_list)
        ratings_list = [ratings.split(";") for ratings in ratings_list]
        ratings_list = np.array(ratings_list, dtype="f")

        new_centroid_ratings = np.mean(ratings_list, axis=0)
        str_centroid_ratings = ";".join(new_centroid_ratings.astype(str, copy=True))

        yield centroid_id, str_centroid_ratings


if __name__ == "__main__":
    UpdateCentroids().run()
