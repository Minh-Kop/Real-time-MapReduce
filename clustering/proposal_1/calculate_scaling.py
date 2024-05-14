from mrjob.job import MRJob
from mrjob.protocol import TextProtocol


class Scaling(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(Scaling, self).configure_args()
        self.add_passthru_arg("--max-value", type=float)

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value

    def reducer(self, user, value):
        max_value = self.options.max_value
        value_scale = float(list(value)[0]) / float(max_value)
        yield user, str(value_scale)


if __name__ == "__main__":
    Scaling().run()
