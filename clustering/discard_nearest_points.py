import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class DiscardNearestPoints(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DiscardNearestPoints, self).configure_args()
        self.add_file_arg(
            '--nearest-points-path', help='Path to M nearest points file')

    def create_nearest_point_list(self, filename):
        nearest_point_list = []
        with open(filename, 'r') as file:
            for line in file:
                user, _ = line.strip().split('\t')
                nearest_point_list.append(float(user))
            file.close()
        return nearest_point_list

    def discard_nearest_points_mapper_init(self):
        self.nearest_point_list = np.array(
            self.create_nearest_point_list(self.options.nearest_points_path), dtype='i')

    def discard_nearest_points_mapper(self, _, line):
        user, value = line.strip().split('\t')
        if not np.isin(int(user), self.nearest_point_list):
            yield user, value

    def steps(self):
        return [
            MRStep(mapper_init=self.discard_nearest_points_mapper_init,
                   mapper=self.discard_nearest_points_mapper,),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '../input_file.txt',  # Tệp đầu vào
    ]
    DiscardNearestPoints().run()
