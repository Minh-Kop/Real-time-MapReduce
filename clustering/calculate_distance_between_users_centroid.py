import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np
from scipy.spatial.distance import cdist


class DistanceBetweenUsersCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DistanceBetweenUsersCentroid, self).configure_args()
        self.add_file_arg('--first-centroid-path',
                          help='Path to init centroid file')

    def distance_between_users_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')
        yield f'{user}', f'{value}'

    def getInitCentroid(self, filename):
        centroids = np.array([])
        col_num = 0
        with open(filename, 'r') as file:
            for line in file:
                _, centroid_value = line.strip().split('\t')
                centroid_value = centroid_value.strip().split('|')
                centroid_value = [el.strip().split(';')
                                  for el in centroid_value]
                centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1]
                centroids = np.append(centroids, centroid_coordinate)
                col_num = centroid_coordinate.size
            file.close()
        centroids = centroids.reshape(-1, col_num)
        return centroids

    def distance_between_users_centroid_reducer_init(self):
        self.centroids = self.getInitCentroid(self.options.first_centroid_path)

    def distance_between_users_centroid_reducer(self, user, value):
        value = list(value)[0].strip().split('|')
        value = [el.strip().split(';') for el in value]
        coordinate = np.array(value, dtype='f')[:, 1].reshape(1, -1)

        centroids = self.centroids
        distances = cdist(coordinate, centroids)
        min_euclidean_distance = np.amin(distances)

        yield user, f'{min_euclidean_distance}'

    def steps(self):
        return [
            MRStep(mapper=self.distance_between_users_centroid_mapper,
                   reducer_init=self.distance_between_users_centroid_reducer_init,
                   reducer=self.distance_between_users_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './output/user_item_matrix.txt',
        '--first-centroid-path', './output/centroids.txt',
    ]
    DistanceBetweenUsersCentroid().run()
