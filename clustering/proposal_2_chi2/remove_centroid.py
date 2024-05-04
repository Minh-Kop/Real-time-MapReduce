import sys
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol

class RemoveCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(RemoveCentroid, self).configure_args()
        self.add_passthru_arg("--centroid", help="Current centroid")

    def mapper(self, _, line):
        user, coord = line.strip().split('\t')
        if not (user == self.options.centroid):
            yield user, coord
        elif len(line.strip().split('-')) == 1:
            yield user, f"{coord}-c"
        else:
            yield user, coord

    def reducer(self, user, coord):
        coord = list(coord)[0]
        yield user, coord

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper,
                reducer=self.reducer,
            ),
        ]

if __name__ == "__main__":
    RemoveCentroid.run()