import os
import sys

import pandas as pd
import numpy as np

sys.path.append(os.path.abspath("./util"))
sys.path.append(os.path.abspath("./clustering/common"))

from custom_util import env_dict, run_mr_job_hadoop
from create_user_item_matrix import UserItemMatrix
from .calculate_avg_and_sum import AvgAndSum
from .calculate_class_probability import ClassProbability
from .calculate_expected_value import ExpectedValue
from .calculate_observed_value import ObservedValue
from .calculate_chi2 import ChiSquare

HADOOP_PATH = env_dict["hadoop_path"]


def run_clustering_chi2(input_file_path, noCluster=3):
    # Number of items
    items_file = open(f"input/items_copy.txt", "r")
    for number_of_items, _ in enumerate(items_file, start=1):
        pass
    items_file.close()
    print(f"Number of items: {number_of_items}")

    # Calculate average rating and sum rating of each user
    run_mr_job_hadoop(
        AvgAndSum,
        [input_file_path, "--n", str(number_of_items)],
        f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
        True,
    )
    print("Calculate average rating and sum rating of each user")

    # Create user-item matrix
    run_mr_job_hadoop(
        UserItemMatrix,
        [
            input_file_path,
            f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
            "--items-path",
            f"{HADOOP_PATH}/input/items_copy.txt",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        True,
    )
    print("Create user-item matrix")

    # Calculate class probability
    run_mr_job_hadoop(
        ClassProbability,
        [f"{HADOOP_PATH}/input/items_copy.txt", "--n", str(number_of_items)],
        f"{HADOOP_PATH}/clustering-chi2-output/class-probability",
        True,
    )
    print("Calculate class probability")

    # Calculate expected value
    run_mr_job_hadoop(
        ExpectedValue,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
            "--class-probability-path",
            f"{HADOOP_PATH}/input/class-probability.txt",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/expected-value",
    )
    print("Calculate expected value")

    # Calculate observed value
    run_mr_job_hadoop(
        ObservedValue,
        [
            f"{HADOOP_PATH}/input/items_copy.txt",
            f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/observed-value",
    )
    print("Calculate observed value")

    # Calculate Chi2
    run_mr_job_hadoop(
        ChiSquare,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/observed-value",
            f"{HADOOP_PATH}/clustering-chi2-output/expected-value",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/chi2-value",
        True,
    )
    print("Calculate Chi2")

    # Define the file paths
    chi2_file_path = f"hadoop_output/chi2-value.txt"
    fult_matrix_path = f"hadoop_output/full-matrix.txt"
    noCluster = 3

    # Read data from file A into a pandas DataFrame
    chi2_df = pd.read_csv(
        chi2_file_path, sep="\t", names=["key", "chi2_value"], dtype={0: np.int32}
    )

    # Read data from file B into a pandas DataFrame
    full_matrix_df = pd.read_csv(
        fult_matrix_path, sep="\t", names=["key", "matrix_value"], dtype={0: np.int32}
    )

    # Merge chi2_df and full_matrix_df on 'key'
    merged_df = pd.merge(chi2_df, full_matrix_df, on="key")

    # Sort merged DataFrame based on 'value_x' and select the top k elements
    centroids_df = merged_df.sort_values(by="chi2_value", ascending=False).head(
        noCluster
    )
    centroids_df = centroids_df.drop(columns=["chi2_value"])
    print(centroids_df)
