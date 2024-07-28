import os
import sys

sys.path.append(os.path.abspath("./util"))

from custom_util import run_mr_job_hadoop, env_dict
from .create_combinations import CreateCombinations
from .create_user_pairs import CreateUserPairs
from .rating_commodity import RatingCommodity
from .rating_usefulness import RatingUsefulness
from .rating_details import RatingDetails
from .rating_time import RatingTime
from .calculate_mfps import MFPS


HADOOP_PATH = env_dict["hadoop_path"]


def run_mfps(input_path, avg_ratings_path, output_path):
    # Create combination
    run_mr_job_hadoop(
        CreateCombinations,
        [input_path],
        f"{HADOOP_PATH}/mfps-output/combinations",
    )
    print("Created combination")

    # Create user pairs
    run_mr_job_hadoop(
        CreateUserPairs,
        [avg_ratings_path, "--users-path", avg_ratings_path],
        f"{HADOOP_PATH}/mfps-output/user-pairs",
    )
    print("Created user pairs")

    # Calculate rating commodity
    run_mr_job_hadoop(
        RatingCommodity,
        [
            f"{HADOOP_PATH}/mfps-output/combinations",
            f"{HADOOP_PATH}/mfps-output/user-pairs",
        ],
        f"{HADOOP_PATH}/mfps-output/rating-commodity",
        True,
    )
    print("Calculated rating commodity")

    # Calculate rating usefulness
    run_mr_job_hadoop(
        RatingUsefulness,
        [
            input_path,
            "--rating-commodity-path",
            f"{HADOOP_PATH}/temp-input/rating-commodity.txt",
        ],
        f"{HADOOP_PATH}/mfps-output/rating-usefulness",
    )
    print("Calculated rating usefulness")

    # Calculate rating detail
    run_mr_job_hadoop(
        RatingDetails,
        [
            f"{HADOOP_PATH}/mfps-output/combinations",
            "--avg-rating-path",
            avg_ratings_path,
        ],
        f"{HADOOP_PATH}/mfps-output/rating-detail",
    )
    print("Calculated rating detail")

    # Calculate rating time
    run_mr_job_hadoop(
        RatingTime,
        [
            f"{HADOOP_PATH}/mfps-output/combinations",
        ],
        f"{HADOOP_PATH}/mfps-output/rating-time",
    )
    print("Calculated rating time")

    # Calculate MFPS
    return_values = run_mr_job_hadoop(
        MFPS,
        [
            f"{HADOOP_PATH}/mfps-output/rating-commodity",
            f"{HADOOP_PATH}/mfps-output/rating-usefulness",
            f"{HADOOP_PATH}/mfps-output/rating-detail",
            f"{HADOOP_PATH}/mfps-output/rating-time",
        ],
        output_path,
    )
    print("Calculated MFPS")

    return return_values
