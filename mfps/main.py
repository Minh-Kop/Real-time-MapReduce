import os
import sys

sys.path.append(os.path.abspath("./util"))

from custom_util import run_mr_job, write_data_to_file
from .create_combinations import CreateCombinations
from .rating_commodity import RatingCommodity
from .rating_usefulness import RatingUsefulness
from .rating_details import RatingDetails
from .rating_time import RatingTime
from .calculate_mfps import MFPS


def run_mfps(input_path, avg_ratings_path, output_path):
    # Create combinations
    result_data = run_mr_job(CreateCombinations, [input_path])
    write_data_to_file(("./mfps/output/create_combinations.txt"), result_data)

    # Calculate rating commodity
    result_data = run_mr_job(
        RatingCommodity,
        [input_path, "--users-path", avg_ratings_path],
    )
    write_data_to_file(("./mfps/output/rating_commodity.txt"), result_data)

    # Calculate rating usefulness
    result_data = run_mr_job(
        RatingUsefulness,
        [
            input_path,
            "--rating-commodity-path",
            "./mfps/output/rating_commodity.txt",
        ],
    )
    write_data_to_file("./mfps/output/rating_usefulness.txt", result_data)

    # Calculate rating details
    result_data = run_mr_job(
        RatingDetails,
        [
            "./mfps/output/create_combinations.txt",
            "--avg-rating-path",
            avg_ratings_path,
        ],
    )
    write_data_to_file("./mfps/output/rating_details.txt", result_data)

    # Calculate rating time
    result_data = run_mr_job(RatingTime, ["./mfps/output/create_combinations.txt"])
    write_data_to_file("./mfps/output/rating_time.txt", result_data)

    # Calculate MFPS
    result_data = run_mr_job(
        MFPS,
        [
            "./mfps/output/rating_commodity.txt",
            "./mfps/output/rating_usefulness.txt",
            "./mfps/output/rating_details.txt",
            "./mfps/output/rating_time.txt",
        ],
    )
    write_data_to_file(output_path, result_data)
    return result_data
