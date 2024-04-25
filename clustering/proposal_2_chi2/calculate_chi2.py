from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol


class ChiSquare(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def calculate_Chi2_mapper(self, _, line):
        user, value = line.strip().split("\t")
        label, value_flag = value.strip().split(";")

        yield f"{user};{label}", value_flag

    def calculate_Chi2_reducer(self, user_label, values):
        user, _ = user_label.strip().split(";")
        E = 0
        O = 0
        for i in list(values):
            val, flag = i.strip().split("|")
            if flag == "e":
                E = float(val)
            else:
                O = float(val)

        yield user, f"{(E-O)**2/E}"

    def sum_Chi2_reducer(self, user, chi2):
        total_sum = sum(map(float, chi2))
        yield user, f"{total_sum}"

    def steps(self):
        return [
            MRStep(
                mapper=self.calculate_Chi2_mapper,
                reducer=self.calculate_Chi2_reducer,
            ),
            MRStep(
                reducer=self.sum_Chi2_reducer,
            ),
        ]


if __name__ == "__main__":
    ChiSquare.run()
