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
    return max_row.iloc[0, 1]


class GetMax(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, _, line):
        user, value = line.strip().split("\t")
        yield None, f"{user};{value}"

    def get_max_value(self, users_values):
        users_values = [line.strip().split(";") for line in users_values]
        users_values = np.array(users_values)

        values = users_values[:, 1]

        max_idx = np.argmax(values)
        return users_values[max_idx]

    def combiner(self, _, users_values):
        user, max_value = self.get_max_value(users_values)
        yield None, f"{user};{max_value}"

    def reducer(self, _, users_values):
        user, max_value = self.get_max_value(users_values)
        yield f"{user}", f"{max_value}"


if __name__ == "__main__":
    GetMax().run()
