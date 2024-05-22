import os
import sys

import pandas as pd
import numpy as np

sys.path.append(os.path.abspath("./util"))
sys.path.append(os.path.abspath("./clustering/common"))

from custom_util import env_dict, run_mr_job_hadoop
from create_user_item_matrix import UserItemMatrix
from create_centroids import CreateCentroids
from calculate_M_nearest_points import MNearestPoints
from kmeans import kmeans
from .calculate_avg_and_sum import AvgAndSum
from .calculate_class_probability import ClassProbability
from .calculate_expected_value import ExpectedValue
from .calculate_observed_value import ObservedValue
from .calculate_chi2 import ChiSquare

HADOOP_PATH = env_dict["hadoop_path"]


def run_clustering_chi2(input_file_path, number_of_clusters=3):
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

    # Get max Chi2
    run_mr_job_hadoop(
        MNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/chi2-value",
            "--M",
            str(number_of_clusters),
            "--is-ascending",
            str(0),
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/top-chi2",
    )
    print("Get top Chi2")

    # Create centroids
    centroids = run_mr_job_hadoop(
        CreateCentroids,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
            f"{HADOOP_PATH}/clustering-chi2-output/top-chi2",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids-0",
    )
    print("Create first centroids")

    return kmeans(0, "clustering-chi2-output", centroids)
