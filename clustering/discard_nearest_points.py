# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class DiscardNearestPoints(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def discard_nearest_points_mapper(self, _, line):
        val_line = line.strip().split('$')
        if(len(val_line)==1):
            user, value = line.strip().split("\t")
        yield f"{int(float(user))}", value

    def discard_nearest_points_reducer(self, user, values):
        values = list(values)
        if len(values) == 1:
            yield user, values[0]

    def steps(self):
        return [
            MRStep(
                mapper=self.discard_nearest_points_mapper,
                reducer=self.discard_nearest_points_reducer,
            )
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     "../input_file.txt",
    # ]
    DiscardNearestPoints().run()
