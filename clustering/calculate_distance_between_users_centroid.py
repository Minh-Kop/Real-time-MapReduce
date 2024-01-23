import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class DistanceBetweenUsersCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol


if __name__ == '__main__':
    sys.argv[1:] = [
        './init_centroid.txt',  # Tệp đầu vào
        './create_user_item_matrix.txt'
        # '--output', 'output1.txt'  # Tệp đầu ra
    ]
    DistanceBetweenUsersCentroid().run()
