from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist


class DistanceBetweenUsersCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DistanceBetweenUsersCentroid, self).configure_args()
        self.add_file_arg("--centroids-path", help="Path to init centroid file")
        self.add_passthru_arg("--return-centroid-id", type=bool, default=False)

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield f"{user}", f"{value}"

    def get_centroid_df(self, filename):
        df = pd.read_csv(filename, sep="\t", names=["user", "ratings"])
        df["ratings"] = df["ratings"].str.split(";")
        df = df.set_index("user")
        return df

    def reducer_init(self):
        self.centroid_df = self.get_centroid_df(self.options.centroids_path)

    def reducer(self, user, value):
        value = list(value)[0].strip()
        ratings = value.split(";")
        ratings = np.array(ratings, dtype="f").reshape(
            1, -1
        )  # Change to 2D array with only 1 row
        centroid_df = self.centroid_df

        distances = cdist(
            ratings, np.array(centroid_df["ratings"].to_list(), dtype="f")
        )[0]
        min_euclidean_distance_index = np.argmin(distances)

        if self.options.return_centroid_id:
            centroid_id = centroid_df.index[min_euclidean_distance_index]
            yield user, f"{value}&{centroid_id}"
        else:
            min_euclidean_distance = distances[min_euclidean_distance_index]
            yield user, f"{min_euclidean_distance}"


if __name__ == "__main__":
    #     import sys
    #
    #     sys.argv[1:] = [
    #         "hadoop_output/c.txt",
    #         "--centroids-path",
    #         "hadoop_output/c.txt",
    #         "--return-centroid-id",
    #         "True",
    #     ]
    DistanceBetweenUsersCentroid().run()
