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
        self.add_passthru_arg(
            '--return-centroid-id', type=bool, default=False)

    def distance_between_users_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')
        yield f'{user}', f'{value}'

    def getInitCentroid(self, filename):
        centroid_ids = np.array([])
        centroids = np.array([])
        col_num = 0
        with open(filename, 'r') as file:
            for line in file:
                centroid_id, centroid_value = line.strip().split('\t')

                centroid_ids = np.append(centroid_ids, centroid_id)

                centroid_value = centroid_value.strip().split('|')
                centroid_value = [el.strip().split(';')
                                  for el in centroid_value]
                centroid_coordinate = np.array(centroid_value, dtype='f')[:, 1]
                centroids = np.append(centroids, centroid_coordinate)

                col_num = centroid_coordinate.size
        centroids = centroids.reshape(-1, col_num)
        return centroid_ids, centroids

    def distance_between_users_centroid_reducer_init(self):
        self.centroid_ids, self.centroids = self.getInitCentroid(
            self.options.first_centroid_path)

    def distance_between_users_centroid_reducer(self, user, value):
        value = list(value)[0].strip()
        coordinate = value.split('|')
        coordinate = [el.strip().split(';') for el in coordinate]
        coordinate = np.array(coordinate, dtype='f')[:, 1].reshape(1, -1)
        centroids = self.centroids
        centroid_ids = self.centroid_ids

        distances = cdist(coordinate, centroids)
        min_euclidean_distance_index = np.argmin(distances)
        centroid_id = centroid_ids[min_euclidean_distance_index]
        min_euclidean_distance = distances[0][min_euclidean_distance_index]

        if self.options.return_centroid_id:
            yield user, f'{value}&{centroid_id}'
        else:
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
