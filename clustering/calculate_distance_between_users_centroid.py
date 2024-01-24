import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class DistanceBetweenUsersCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(DistanceBetweenUsersCentroid, self).configure_args()
        self.add_file_arg('--init-centroid-path',
                          help='Path to init centroid file')

    def getInitCentroid(self, filename):
        f = open(filename, 'r')
        user = f.readline().strip().split('\t')
        f.close()
        return user

    def distance_between_users_centroid_mapper_init(self):
        init_centroid_path = self.options.init_centroid_path
        result = self.getInitCentroid(init_centroid_path)
        self.init_centroid_id = result[0]
        self.init_centroid_value = result[1]

    def distance_between_users_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')
        if user != self.init_centroid_id:
            yield f'{user}', f'{value}'

    def distance_between_users_centroid_reducer(self, user, value):
        value = list(value)[0].strip().split('|')
        value = [el.strip().split(';') for el in value]
        coordinate = np.array(value, dtype='f')

        centroid_value = self.init_centroid_value
        centroid_value = centroid_value.strip().split('|')
        centroid_value = [el.strip().split(';') for el in centroid_value]
        centroid_coordinate = np.array(centroid_value, dtype='f')

        euclidean_distance = abs(np.linalg.norm(
            centroid_coordinate - coordinate))
        yield user, euclidean_distance

    def steps(self):
        return [
            MRStep(mapper_init=self.distance_between_users_centroid_mapper_init,
                   mapper=self.distance_between_users_centroid_mapper,
                   reducer=self.distance_between_users_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '--init-centroid', './init_centroid.txt',
        './create_user_item_matrix.txt'
    ]
    DistanceBetweenUsersCentroid().run()
