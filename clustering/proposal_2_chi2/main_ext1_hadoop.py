import os
import sys

import pandas as pd
import numpy as np

sys.path.append(os.path.abspath("./util"))
sys.path.append(os.path.abspath("./clustering/common"))

from custom_util import env_dict, run_mr_job_hadoop
from create_user_item_matrix import UserItemMatrix
from calculate_M_nearest_points import MNearestPoints
from create_centroids import CreateCentroids
from get_max import GetMax
from kmeans import kmeans
from .calculate_avg_and_sum import AvgAndSum
from .calculate_class_probability import ClassProbability
from .calculate_expected_value import ExpectedValue
from .calculate_observed_value import ObservedValue
from .calculate_chi2 import ChiSquare
from .remove_centroid import RemoveCentroid
from .calculate_distance_between_centroids import DistanceBetweenCentroids
from .filter_centroids import FilterCentroids

HADOOP_PATH = env_dict["hadoop_path"]


def run_clustering_chi2_ext1(
    input_file_path, number_of_clusters=2, number_of_multiplications=1
):
    # Number of items
    # items_file = open(f"input/items_copy.txt", "r")
    items_file = open(f"input/items.txt", "r")
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

    # Split average ratings to a separate file
    run_mr_job_hadoop(
        FilterCentroids,
        [f"{HADOOP_PATH}/clustering-chi2-output/avg-sum", "--spilt-string", "|a"],
        f"{HADOOP_PATH}/clustering-chi2-output/avg-ratings",
        True,
    )
    print("Split average ratings to a separate file")

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

    # Calculate chi2
    run_mr_job_hadoop(
        ChiSquare,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/observed-value",
            f"{HADOOP_PATH}/clustering-chi2-output/expected-value",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/chi2-value",
    )
    print("Calculate Chi2")

    # Get max Chi2
    run_mr_job_hadoop(
        MNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/chi2-value",
            "--M",
            str(number_of_clusters * number_of_multiplications),
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
        f"{HADOOP_PATH}/clustering-chi2-output/centroids",
    )
    print("Create first centroids")

    # Get max centroid
    max_centroid = centroids[0].replace("\n", "").split("\t")

    # Remove current centroid
    run_mr_job_hadoop(
        RemoveCentroid,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/centroids",
            "--centroid",
            max_centroid[0],
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids-0",
    )
    print("Remove centroid")

    # Loop
    for i in range(number_of_clusters - 1):
        print(f"\nLoop: {i}")

        # Calculate distance between current to others centroids
        run_mr_job_hadoop(
            DistanceBetweenCentroids,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i}",
                "--centroid-coord",
                max_centroid[1],
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/distances-{i}",
        )
        print("Calculate distance between centroids")

        # Get highest centroid
        run_mr_job_hadoop(
            GetMax,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/distances-{i}",
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/top-centroid-id-{i}",
        )

        max_centroid = (
            run_mr_job_hadoop(
                CreateCentroids,
                [
                    f"{HADOOP_PATH}/clustering-chi2-output/centroids",
                    f"{HADOOP_PATH}/clustering-chi2-output/top-centroid-id-{i}",
                ],
            )[0]
            .replace("\n", "")
            .split("\t")
        )
        print("Get highest centroid")

        # Remove highest centroid
        run_mr_job_hadoop(
            RemoveCentroid,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i}",
                "--centroid",
                max_centroid[0],
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i + 1}",
        )
        print("Remove highest centroid")

    # Filter only centroids
    centroids = run_mr_job_hadoop(
        FilterCentroids,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/centroids-{number_of_clusters - 1}",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids-{number_of_clusters}",
        True,
    )
    print("Filter only centroids")

    return kmeans(number_of_clusters, "clustering-chi2-output", centroids)
