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
        return user

    def distance_between_users_centroid_mapper_init(self):
        init_centroid_path = self.options.init_centroid_path
        self.init_centroid_id, self.init_centroid_value = self.getInitCentroid(
            init_centroid_path)

    def distance_between_users_centroid_mapper(self, _, line):
        user, value = line.strip().split('\t')

        if user != self.init_centroid_id:
            yield f'{self.init_centroid_id};{user}', f'{self.init_centroid_value};{value}'

    def distance_between_users_centroid_reducer(self, users, values):
        value = np.array(values)[0]
        # centroid_value, user_value = value.strip().split(';')
        # centroid_value = [val.strip().split('|') for val in centroid_value]
        # user_value = [val.strip().split('|') for val in user_value]
        # centroid_value = np.array(centroid_value).astype(int)
        # user_value = np.array(user_value).astype(int)

        # euclidean_distance = abs(np.linalg.norm(centroid_value - user_value))

        yield users, value

    def steps(self):
        return [
            MRStep(mapper_init=self.distance_between_users_centroid_mapper_init,
                   mapper=self.distance_between_users_centroid_mapper,
                   reducer=self.distance_between_users_centroid_reducer)
        ]


if __name__ == '__main__':
    sys.argv[1:] = [
        '--init-centroid', './init_centroid.txt',  # Tệp đầu vào
        './create_user_item_matrix.txt'
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    DistanceBetweenUsersCentroid().run()
