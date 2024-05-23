from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


class DistanceBetweenCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DistanceBetweenCentroids, self).configure_args()
        self.add_passthru_arg("--centroid-coord", help="Current centroid coord")

    def mapper(self, _, line):
        flag = line.strip().split("-")
        if len(flag) == 1:
            user, coord = line.strip().split("\t")
            yield user, coord

    def reducer(self, user, coord):
        coord = list(coord)[0]
        centroid_coord = self.options.centroid_coord
        if coord != centroid_coord:
            coord_ = np.array(
                [
                    float(coord_.strip().split(";")[1])
                    for coord_ in (coord.strip().split("|"))
                ]
            )
            centroid_coord_ = np.array(
                [
                    float(centroid_coord_.strip().split(";")[1])
                    for centroid_coord_ in (centroid_coord.strip().split("|"))
                ]
            )

            dist = np.linalg.norm(centroid_coord_ - coord_)

            yield user, f"{dist}"


if __name__ == "__main__":
    DistanceBetweenCentroids.run()
