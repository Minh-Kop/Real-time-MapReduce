# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class Scaling(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(Scaling, self).configure_args()
        self.add_file_arg("--max-value-path", help="Path to most max value file")

    def scaling_mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield user, value

    def getMax(self, filename):
        with open(filename, "r") as file:
            line = file.readline()
            _, value_max = line.strip().split("\t")

        return value_max

    def scaling_reducer_init(self):
        max_value_path = self.options.max_value_path
        self.value_max = self.getMax(max_value_path)

    def scaling_reducer(self, user, value):
        value_scale = float(list(value)[0]) / float(self.value_max)
        yield user, str(value_scale)

    def steps(self):
        return [
            MRStep(
                mapper=self.scaling_mapper,
                reducer_init=self.scaling_reducer_init,
                reducer=self.scaling_reducer,
            ),
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     './output/importances.txt',
    #     '--max-value-path', './output/new_scale_file.txt',
    # ]
    Scaling().run()
