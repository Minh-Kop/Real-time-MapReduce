from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class RemoveCentroid(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(RemoveCentroid, self).configure_args()
        self.add_passthru_arg("--centroid", help="Current centroid")

    def mapper(self, _, line):
        user, coord = line.strip().split("\t")
        if not (user == self.options.centroid):
            yield user, coord
        elif len(coord.strip().split("-")) == 1:
            yield user, f"{coord}-c"
        else:
            yield user, coord


if __name__ == "__main__":
    RemoveCentroid.run()
