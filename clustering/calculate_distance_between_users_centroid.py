import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np

first_centroid_id = '1'
first_centroid_value = '1'


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
        f = open(filename, 'r')
        user = f.readline().strip().split('\t')
        f.close()
        return user

    def distance_between_users_centroid_reducer_init(self):
        first_centroid_path = self.options.first_centroid_path
        self.first_centroid_id, self.first_centroid_value = self.getInitCentroid(
            first_centroid_path)

    def distance_between_users_centroid_reducer(self, user, value):
        value = list(value)[0].strip().split('|')
        value = [el.strip().split(';') for el in value]
        coordinate = np.array(value, dtype='f')

        centroid_value = self.first_centroid_value
        centroid_value = centroid_value.strip().split('|')
        centroid_value = [el.strip().split(';') for el in centroid_value]
        centroid_coordinate = np.array(centroid_value, dtype='f')

        euclidean_distance = abs(np.linalg.norm(
            centroid_coordinate - coordinate))
        yield user, f'{euclidean_distance}'

    def steps(self):
        return [
            MRStep(mapper=self.distance_between_users_centroid_mapper,
                   reducer_init=self.distance_between_users_centroid_reducer_init,
                   reducer=self.distance_between_users_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        './output/user_item_matrix.txt',
        '--first-centroid-path', './output/first_centroid.txt',
    ]
    DistanceBetweenUsersCentroid().run()
