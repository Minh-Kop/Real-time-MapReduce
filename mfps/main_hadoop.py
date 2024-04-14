import os
import sys

sys.path.append(os.path.abspath("./util"))

from custom_util import run_mr_job_hadoop
from .create_combinations import CreateCombinations
from .rating_commodity import RatingCommodity
from .rating_usefulness import RatingUsefulness
from .rating_details import RatingDetails
from .rating_time import RatingTime
from .calculate_mfps import MFPS


def run_mfps(input_path, avg_ratings_path, output_path):
    # Create combination
    run_mr_job_hadoop(
        CreateCombinations,
        [input_path],
        "hdfs://localhost:9000/user/mackop/mfps-output/combination",
    )
    print("Create combination")

    # Calculate rating commodity
    run_mr_job_hadoop(
        RatingCommodity,
        [input_path, "--users-path", avg_ratings_path],
        "hdfs://localhost:9000/user/mackop/mfps-output/rating-commodity",
        True,
    )
    print("Calculate rating commodity")

    # Calculate rating usefulness
    run_mr_job_hadoop(
        RatingUsefulness,
        [
            input_path,
            "--rating-commodity-path",
            "hdfs://localhost:9000/user/mackop/input/rating-commodity.txt",
        ],
        "hdfs://localhost:9000/user/mackop/mfps-output/rating-usefulness",
    )
    print("Calculate rating usefulness")

    # Calculate rating detail
    run_mr_job_hadoop(
        RatingDetails,
        [
            "hdfs://localhost:9000/user/mackop/mfps-output/combination",
            "--avg-rating-path",
            avg_ratings_path,
        ],
        "hdfs://localhost:9000/user/mackop/mfps-output/rating-detail",
    )
    print("Calculate rating detail")

    # Calculate rating time
    run_mr_job_hadoop(
        RatingTime,
        [
            "hdfs://localhost:9000/user/mackop/mfps-output/combination",
        ],
        "hdfs://localhost:9000/user/mackop/mfps-output/rating-time",
    )
    print("Calculate rating time")

    # Calculate MFPS
    return_values = run_mr_job_hadoop(
        MFPS,
        [
            "hdfs://localhost:9000/user/mackop/mfps-output/rating-commodity",
            "hdfs://localhost:9000/user/mackop/mfps-output/rating-usefulness",
            "hdfs://localhost:9000/user/mackop/mfps-output/rating-detail",
            "hdfs://localhost:9000/user/mackop/mfps-output/rating-time",
        ],
        output_path,
    )
    print("Calculate MFPS")

    return return_values
