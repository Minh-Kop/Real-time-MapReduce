# import sys

from mrjob.job import MRJob
from mrjob.protocol import TextProtocol
import pandas as pd


class RatingDetails(MRJob):
    OUTPUT_PROTOCOL = TextProtocol

    def configure_args(self):
        super(RatingDetails, self).configure_args()
        self.add_file_arg("--avg-rating-path", help="Path to average rating file")

    def mapper(self, _, line):
        key, value = line.strip().split("\t")
        ratings = value.strip().split("|")[0]
        yield key, ratings

    def reducer_init(self):
        avg_rating_path = self.options.avg_rating_path
        self.avg_rating_df = pd.read_csv(
            avg_rating_path, sep="\t", names=["user", "avg_rating"], index_col=0
        )

    def reducer(self, users, values):
        user_1, user_2 = users.strip().split(";")
        avg_rating_df = self.avg_rating_df
        threshold_1 = avg_rating_df.loc[int(float(user_1)), "avg_rating"]
        threshold_2 = avg_rating_df.loc[int(float(user_2)), "avg_rating"]

        count = 0
        for ratings in values:
            rating_1, rating_2 = ratings.strip().split(";")
            rating_1 = float(rating_1)
            rating_2 = float(rating_2)

            if (rating_1 < threshold_1 and rating_2 < threshold_2) or (
                rating_1 > threshold_1 and rating_2 > threshold_2
            ):
                count = count + 1

        yield f"{user_1};{user_2}", f"{count};rd"
        yield f"{user_2};{user_1}", f"{count};rd"


if __name__ == "__main__":
    # sys.argv[1:] = [
    #     "mfps/output/create_combinations.txt",
    #     "--avg-rating-path",
    #     "mfps/output/avg_ratings.txt",
    # ]
    RatingDetails().run()
