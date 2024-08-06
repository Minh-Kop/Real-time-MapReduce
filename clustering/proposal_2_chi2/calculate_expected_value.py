from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import pandas as pd


class ExpectedValue(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(ExpectedValue, self).configure_args()
        self.add_file_arg(
            "--categories-probability-path",
            help="Path to the categories probability file",
        )

    def mapper(self, _, line):
        user, values = line.strip().split("\t")
        value, flag = values.strip().split("|")
        if flag == "s":
            yield user, value

    def reducer_init(self):
        categories_probability_path = self.options.categories_probability_path
        self.class_probabilities = pd.read_csv(
            categories_probability_path, sep="\t", names=["categories", "probability"]
        )

    def reducer(self, user, sum):
        sum = float(list(sum)[0])

        for categories, probability in self.class_probabilities.itertuples(index=False):
            expected_value = sum * probability
            yield f"{user};{categories}", f"{expected_value}|e"


if __name__ == "__main__":
    ExpectedValue.run()
