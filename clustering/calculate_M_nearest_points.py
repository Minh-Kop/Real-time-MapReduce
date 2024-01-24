import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class MNearestPoints(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(MNearestPoints, self).configure_args()
        self.add_file_arg(
            '--m-path', help='Path to number of discard points file')

    def calculate_M_nearest_points_mapper(self, _, line):
        user, distance = line.strip().split('\t')
        yield None, f'{user};{distance}'

    def create_M(self, filename):
        M = 0
        with open(filename, 'r') as file:
            for line in file:
                M = int(line.strip())
        return M

    def calculate_M_nearest_points_reducer_init(self):
        m_path = self.options.m_path
        self.M = self.create_M(m_path)

    def calculate_M_nearest_points_reducer(self, _, distances):
        distances = [line.strip().split(';') for line in distances]
        distances = np.array(distances, dtype='f')

        # Get the indices that would sort the array based on the second column
        indices = np.argsort(distances[:, 1])

        # Use the indices to sort the array
        sorted_distances = distances[indices]

        M = self.M
        if (M == 0):
            return

        # Get top M values in sorted array
        nearest_points = sorted_distances[-M:]

        for user, distance in nearest_points:
            yield f'{user}', f'{distance}'

    def steps(self):
        return [
            MRStep(mapper=self.calculate_M_nearest_points_mapper,
                   reducer_init=self.calculate_M_nearest_points_reducer_init,
                   reducer=self.calculate_M_nearest_points_reducer),
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        'D.txt',
        '--m-path', 'calculate_number_of_discard_points.txt',
    ]
    MNearestPoints().run()
