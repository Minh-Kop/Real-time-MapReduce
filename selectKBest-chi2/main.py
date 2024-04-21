import os
import sys

sys.path.append(os.path.abspath("./util"))

from create_user_item_matrix import UserItemMatrix
from custom_util import run_mr_job_hadoop
from calculate_avg_and_sum import AvgAndSum
from calculate_class_probability import ClassProbability
from calculate_expected_value import ExpectedValue
from calculate_observed_value import ObservedValue
from calculate_chi2 import ChiSquare
import pandas as pd

def run_clustering_chi2(input_path, noCluster):
    HADOOP_PATH = env_dict["hadoop_path"]

    # Number of items
    item_df = pd.read_csv(f"{HADOOP_PATH}/input/items.txt", names=['items'])
    noItem = item_df['items'].count()

    # Calculate average rating and sum rating of each user
    run_mr_job_hadoop(
        AvgAndSum,
        [input_path, "--n", noItem],
        f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
        True
    )

    # Create user x item matrix
    run_mr_job_hadoop(
        UserItemMatrix,
        [
            input_file_path,
            f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
            "--items-path",
            f"{HADOOP_PATH}/input/items.txt",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        True
    )

    # Calculate class probability
    run_mr_job_hadoop(
        ClassProbability,
        [
            f"{HADOOP_PATH}/input/items.txt",
            "--n",
            noItem
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/class-probability",
        True
    )

    # Calculate observed value
    run_mr_job_hadoop(
        ObservedValue,
        [
            f"{HADOOP_PATH}/input/items.txt",
            f"{HADOOP_PATH}/clustering-chi2-output/user-item-matrix"
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/observed-value",
        True
    )

    # Calculate expected value
    run_mr_job_hadoop(
        ExpectedValue,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
            "--cProb",
            f"{HADOOP_PATH}/clustering-chi2-output/class-probability",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/expected-value",
        True
    )

    # Calculate Chi2
    run_mr_job_hadoop(
        ChiSquare,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/observed-value",
            f"{HADOOP_PATH}/clustering-chi2-output/expected-value",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/chi2-value",
        True
    )

    # Define the file paths
    file_A = f"{HADOOP_PATH}/clustering-chi2-output/chi2-value.txt"
    file_B = f"{HADOOP_PATH}/clustering-chi2-output/full-matrix.txt"

    # Read data from file A into a pandas DataFrame
    data_A_df = pd.read_csv(file_A, sep="\t", header=None, names=["key", "value"])

    # Read data from file B into a pandas DataFrame
    data_B_df = pd.read_csv(file_B, sep="\t", header=None, names=["key", "value"])

    # Convert 'key' column to integer type
    data_B_df["key"] = data_B_df["key"].astype(int)

    # Merge data_A_df and data_B_df on 'key'
    merged_df = pd.merge(data_A_df, data_B_df, on="key", how="inner")

    # Sort merged DataFrame based on 'value_x' and select the top k elements
    sorted_merged_df = merged_df.sort_values(by="value_x", ascending=False).head(noCluster)

    # Retrieve corresponding values from sorted_merged_df
    corresponding_values = sorted_merged_df[["key", "value_y"]].values.tolist()

    print(corresponding_values)