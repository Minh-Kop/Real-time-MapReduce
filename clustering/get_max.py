import pandas as pd
from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import numpy as np


def get_max(input_path, output_path):
    df = pd.read_csv(
        input_path,
        sep="\t",
        names=["user", "distance"],
        dtype={"user": "Int64", "distance": "Float64"},
    )
    max_idx = df["distance"].idxmax()
    max_row = df.loc[[max_idx]]
    max_row.to_csv(output_path, sep="\t", index=False, header=False)


class GetMax(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield None, f"{user};{value}"

    def combiner(self, _, values):
        values = [line.strip().split(";") for line in values]
        values = np.array(values)

        list = values[:, 1]

        index = np.argmax(list)
        max = values[index]
        user, max_value = max
        yield None, f"{user};{max_value}"

    def reducer(self, _, values):
        values = [line.strip().split(";") for line in values]
        values = np.array(values)

        list = values[:, 1]

        index = np.argmax(list)
        max = values[index]
        user, max_value = max
        yield f"{user}", f"{max_value}"


if __name__ == "__main__":
    GetMax().run()
