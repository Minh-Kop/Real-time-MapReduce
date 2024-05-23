from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class FilterCentroids(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(FilterCentroids, self).configure_args()
        self.add_passthru_arg(
            "--spilt-string", type=str, default="-c", help="Split string to filter"
        )

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        spilt_string = self.options.spilt_string
        value = value.strip().split(spilt_string)
        if len(value) > 1:
            yield user, value[0]


if __name__ == "__main__":
    FilterCentroids.run()
