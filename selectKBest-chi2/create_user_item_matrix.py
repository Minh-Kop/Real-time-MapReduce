# import sys

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import TextProtocol
import numpy as np


class UserItemMatrix(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def create_user_item_matrix_mapper(self, _, line):
        key, value = line.strip().split("\t")
        key = key.strip().split(";")
        if len(key) == 1:
            val, flag = value.strip().split('|')
            if(flag == 'a'):
                yield key[0], val
            return
        user, item = key
        rating = value.strip().split(";")[0]

        yield user, f"{item};{rating}"

    def configure_args(self):
        super(UserItemMatrix, self).configure_args()
        self.add_file_arg("--items-path", help="Path to the items file")

    def create_item_list(self, filename):
        items = []
        with open(filename, "r") as file:
            for line in file:
                item = line.strip()  # Remove leading/trailing whitespaces and newlines
                items.append(float(item))
        return items

    def create_user_item_matrix_reducer_init(self):
        items_path = self.options.items_path
        self.items = self.create_item_list(items_path)

    def create_user_item_matrix_reducer(self, user, values):
        values = [value.strip().split(";") for value in values]
        values = np.array(values, dtype="object")
        # Find rows with length 1
        rows_to_remove = np.array([len(row) == 1 for row in values])

        # Use boolean indexing to create a new array with rows of length 1
        removed_rows = values[rows_to_remove]
        avg_rating = removed_rows[0][0]

        # Use boolean indexing to remove rows from the original array
        coordinates = values[~rows_to_remove]
        coordinates = np.vstack(coordinates).astype(float)

        result = []
        for item in self.items:
            found = False
            for user_item, rating in coordinates:
                if float(user_item) == item:
                    result.append(f"{item};{rating}")
                    found = True
                    break
            if not found:
                result.append(f"{item};{avg_rating}")
        result = "|".join(result)
        yield user, result

    def steps(self):
        return [
            MRStep(
                mapper=self.create_user_item_matrix_mapper,
                reducer_init=self.create_user_item_matrix_reducer_init,
                reducer=self.create_user_item_matrix_reducer,
            )
        ]


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     "./input/input_file_copy.txt",
    #     "./clustering/output/avg_ratings.txt",
    #     "--items-path",
    #     "./input/items.txt",
    #     "-r",
    #     "hadoop",
    # ]
    UserItemMatrix().run()
