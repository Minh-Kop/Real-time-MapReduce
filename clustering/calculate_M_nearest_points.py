# import sys

import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


def M_nearest_points_pandas(input_path, M, output_path):
    if M == 0:
        return

    df = pd.read_csv(
        input_path,
        sep="\t",
        names=["user", "distance"],
        dtype={"user": "Int64", "distance": "Float64"},
    )
    df = df.sort_values(["distance"], ascending=False)
    df = df.iloc[:M]
    df.to_csv(output_path, sep="\t", index=False, header=False)


class MNearestPoints(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MNearestPoints, self).configure_args()
        self.add_passthru_arg("--M", type=int, default=1)

    def mapper(self, _, line):
        user, distance = line.strip().split("\t")
        yield None, f"{user};{distance}"

    def combiner(self, _, distances):
        distances = [line.strip().split(";") for line in distances]
        distances = np.array(distances, dtype="f")

        # Get the indices that would sort the array based on the second column
        indices = np.argsort(distances[:, 1])

        # Use the indices to sort the array
        sorted_distances = distances[indices]

        M = self.options.M
        if M == 0:
            return

        # Get top M values in sorted array
        nearest_points = sorted_distances[:M]
        for user, distance in nearest_points:
            yield None, f"{user};{distance}"

    def reducer(self, _, distances):
        distances = [line.strip().split(";") for line in distances]
        distances = np.array(distances, dtype="f")

        # Get the indices that would sort the array based on the second column
        indices = np.argsort(distances[:, 1])

        # Use the indices to sort the array
        sorted_distances = distances[indices]

        M = self.options.M
        if M == 0:
            return

        # Get top M values in sorted array
        nearest_points = sorted_distances[:M]
        for user, distance in nearest_points:
            yield f"{user}", f"{distance}"


if __name__ == "__main__":
    M = 1

    # M_nearest_points_pandas(
    #     "./clustering/output/D.txt", M, "./clustering/output/M_nearest_points.txt"
    # )

    # sys.argv[1:] = [
    #     "./clustering/output/D.txt",
    #     "--M",
    #     str(M),
    # ]
    MNearestPoints().run()
