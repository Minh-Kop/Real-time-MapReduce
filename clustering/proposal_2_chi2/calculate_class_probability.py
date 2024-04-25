from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class ClassProbability(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(ClassProbability, self).configure_args()
        self.add_passthru_arg("--n", type=int, help="Number of items")

    def mapper(self, _, line):
        item, label = line.strip().split("\t")
        yield label, item

    def reducer(self, label, items):
        number_of_all_items = self.options.n
        number_of_label_items = len(list(items))

        yield label, f"{float(number_of_label_items) / number_of_all_items}"


if __name__ == "__main__":
    ClassProbability.run()
