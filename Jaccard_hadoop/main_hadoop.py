import os
import sys

sys.path.append(os.path.abspath("./util"))

from custom_util import env_dict, run_mr_job_hadoop
from .count_item import CountItem
from .intersection import Intersection
from .jaccard import Jaccard

HADOOP_PATH = env_dict["hadoop_path"]


def run_jaccard(input_file_path):
    print("Calculate CountItem")
    run_mr_job_hadoop(
        CountItem,
        [input_file_path],
        f"{HADOOP_PATH}/jaccard-output/count_item",
    )
    print("CountItem successful")

    print("Intersection CountItem")
    run_mr_job_hadoop(
        Intersection,
        [input_file_path],
        f"{HADOOP_PATH}/jaccard-output/intersection",
    )
    print("Intersection successful")

    print("Calculate Jaccard")
    return_values = run_mr_job_hadoop(
        Jaccard,
        [
            f"{HADOOP_PATH}/jaccard-output/count_item",
            f"{HADOOP_PATH}/jaccard-output/intersection",
        ],
        f"{HADOOP_PATH}/jaccard-output/jaccard",
    )
    print("Jaccard successful")

    return return_values
