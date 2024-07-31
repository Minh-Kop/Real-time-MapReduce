from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


class DistanceBetweenCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DistanceBetweenCentroids, self).configure_args()
        self.add_passthru_arg("--centroid-ratings", help="Current centroid ratings")

    def mapper(self, _, line):
        flag = line.strip().split("-")
        if len(flag) == 1:
            user, ratings = line.strip().split("\t")
            yield user, ratings

    def reducer(self, user, ratings):
        ratings = list(ratings)[0]
        centroid_ratings = self.options.centroid_ratings

        ratings = np.array(ratings.strip().split(";"), dtype="f")
        centroid_ratings = np.array(centroid_ratings.strip().split(";"), dtype="f")
        distance = np.linalg.norm(centroid_ratings - ratings)

        yield user, str(distance)


if __name__ == "__main__":
    #     import sys
    #
    #     sys.argv[1:] = [
    #         "hadoop_output/centroids-0.txt",
    #         "--centroid-ratings",
    #         "1.0;5.0;3.0",
    #     ]
    DistanceBetweenCentroids.run()
