import os
import sys

sys.path.append(os.path.abspath("./util"))

from custom_util import env_dict, run_mr_job_hadoop, write_data_to_file
from .count_item import CountItem
from .calculate_intersection import CalculateIntersection
from .jaccard import Jaccard

HADOOP_PATH = env_dict["hadoop_path"]


def run_jaccard(input_file_path, user_file_path, output_file_path=None):
    # Calculate number of items of each user
    run_mr_job_hadoop(
        CountItem,
        [input_file_path],
        f"{HADOOP_PATH}/jaccard-output/count_items",
    )
    print("Calculated number of items of each user")

    # Calculate number of intersected items of each pair of users
    run_mr_job_hadoop(
        CalculateIntersection,
        [input_file_path, "--users-path", user_file_path],
        f"{HADOOP_PATH}/jaccard-output/intersection",
    )
    print("Calculated number of intersected items of each pair of users")

    # Calculate Jaccard
    return_values = run_mr_job_hadoop(
        Jaccard,
        [
            f"{HADOOP_PATH}/jaccard-output/count_items",
            f"{HADOOP_PATH}/jaccard-output/intersection",
        ],
        f"{HADOOP_PATH}/jaccard-output/jaccard",
    )
    if output_file_path:
        write_data_to_file(output_file_path, return_values)
    print("Calculated Jaccard")
