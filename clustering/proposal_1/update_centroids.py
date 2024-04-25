# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class UpdateCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def update_centroids_mapper(self, _, line):
        _, value = line.strip().split("\t")
        coordinate, centroid_id = value.split("&")
        yield f"{centroid_id}", f"{coordinate}"

    def update_centroids_reducer(self, centroid_id, coordinates):
        coordinates = [coordinate.strip().split("|") for coordinate in coordinates]
        for index, coordinate in enumerate(coordinates):
            coordinates[index] = [el.strip().split(";") for el in coordinate]
        coordinates = np.array(coordinates, dtype="f")

        new_coordinate = np.mean(coordinates, axis=0)

        str_coordinate = ""
        for el in new_coordinate:
            str_coordinate += ";".join(el.astype(str, copy=True)) + "|"

        yield f"{centroid_id}", f"{str_coordinate[:-1]}"

    def steps(self):
        return [
            MRStep(
                mapper=self.update_centroids_mapper,
                reducer=self.update_centroids_reducer,
            ),
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     "./importance.txt",
    # ]
    UpdateCentroids().run()
