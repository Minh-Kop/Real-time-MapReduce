import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np
import pandas as pd
import math

class DisatanceBetweenCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DisatanceBetweenCentroids, self).configure_args()
        self.add_passthru_arg("--centroid-coord", help="Curr centroid coord")

    def mapper(self, _, line):
        flag = line.strip().split('-')
        if len(flag) == 1:
            user, coord = line.strip().split('\t')
            yield user, coord

    def reducer(self, user, coord):
        coord = list(coord)[0]
        ccorrd = self.options.centroid_coord
        if not (coord==ccorrd):
            coor = np.array([float(coor.strip().split(';')[1]) for coor in (coord.strip().split('|'))])
            cur_coor = np.array([float(cur_coor.strip().split(';')[1]) for cur_coor in (ccorrd.strip().split('|'))])

            dist = np.linalg.norm(cur_coor - coor)

            yield user, f"{dist}"

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            ),
        ]