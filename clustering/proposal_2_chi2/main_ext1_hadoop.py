import os
import sys


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
from .remove_items_from_matrix import RemoveItemsFromMatrix
from .calculate_chi2 import ChiSquare
from .remove_centroid import RemoveCentroid
from .calculate_distance_between_centroids import DistanceBetweenCentroids
from .filter_centroids import FilterCentroids

HADOOP_PATH = env_dict["hadoop_path"]


def run_clustering_chi2_ext1(
    input_file_path,
    item_file_path,
    hdfs_item_file_path,
    number_of_clusters=3,
    number_of_multiplications=1,
):
    # Number of items
    items_file = open(item_file_path, "r")
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
    print("Calculated average rating and sum rating of each user")

    # Split average ratings to a separate file
    run_mr_job_hadoop(
        FilterCentroids,
        [f"{HADOOP_PATH}/clustering-chi2-output/avg-sum", "--spilt-string", "|a"],
        f"{HADOOP_PATH}/clustering-chi2-output/avg-ratings",
        True,
    )
    print("Splitted average ratings to a separate file")

    # Create user-items matrix
    run_mr_job_hadoop(
        UserItemMatrix,
        [
            input_file_path,
            f"{HADOOP_PATH}/clustering-chi2-output/avg-ratings",
            "--items-path",
            hdfs_item_file_path,
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        True,
    )
    print("Created user-items matrix")

    # Calculate class probability
    run_mr_job_hadoop(
        ClassProbability,
        [hdfs_item_file_path, "--n", str(number_of_items)],
        f"{HADOOP_PATH}/clustering-chi2-output/class-probability",
        True,
    )
    print("Calculated class probability")

    # Calculate expected value
    run_mr_job_hadoop(
        ExpectedValue,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/avg-sum",
            "--categories-probability-path",
            f"{HADOOP_PATH}/temp-input/class-probability.txt",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/expected-values",
        True,
    )
    print("Calculated expected value")

    # Calculate observed value
    run_mr_job_hadoop(
        ObservedValue,
        [
            hdfs_item_file_path,
            f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/observed-values",
        True,
    )
    print("Calculated observed value")

    # Calculate chi2
    run_mr_job_hadoop(
        ChiSquare,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/observed-values",
            f"{HADOOP_PATH}/clustering-chi2-output/expected-values",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/chi2-values",
        True,
    )
    print("Calculated Chi2")

    # Get users with top M Chi2 values
    run_mr_job_hadoop(
        MNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/chi2-values",
            "--M",
            str(number_of_clusters * number_of_multiplications),
            "--is-ascending",
            str(0),
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/top-chi2",
        True,
    )
    print("Got users with top M Chi2 values")

    # Remove items from user-items matrix
    run_mr_job_hadoop(
        RemoveItemsFromMatrix,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/full-matrix",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/matrix",
        True,
    )
    print("Removed items from user-items matrix")

    # Connect top M users with their ratings
    centroids = run_mr_job_hadoop(
        CreateCentroids,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/matrix",
            f"{HADOOP_PATH}/clustering-chi2-output/top-chi2",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids",
        True,
    )
    print("Connected top M users with their ratings")

    # Get max centroid id
    run_mr_job_hadoop(
        GetMax,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/top-chi2",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/top-chi2-centroid-id",
        True,
    )
    print("Got max centroid id")

    # Connect max centroid id with its ratings
    max_centroid = (
        run_mr_job_hadoop(
            CreateCentroids,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/centroids",
                f"{HADOOP_PATH}/clustering-chi2-output/top-chi2-centroid-id",
            ],
        )[0]
        .replace("\n", "")
        .split("\t")
    )
    print("Connected max centroid id with its ratings")

    # Remove current centroid
    run_mr_job_hadoop(
        RemoveCentroid,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/centroids",
            "--centroid",
            max_centroid[0],
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids-0",
        True,
    )
    print("Removed current centroid")

    # Loop to find centroids
    for i in range(number_of_clusters - 1):
        print(f"\nLoop centroid {i}")

        # Calculate the distances between the current centroid and others
        run_mr_job_hadoop(
            DistanceBetweenCentroids,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i}",
                "--centroid-ratings",
                max_centroid[1],
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/distances-{i}",
            True,
        )
        print("Calculated the distances between the current centroid and others")

        # Get centroid id having the furthest distance
        run_mr_job_hadoop(
            GetMax,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/distances-{i}",
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/top-centroid-id-{i}",
            True,
        )
        print("Got centroid id having the furthest distance")

        # Connect the furthest distance centroid id with its ratings
        max_centroid = (
            run_mr_job_hadoop(
                CreateCentroids,
                [
                    f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i}",
                    f"{HADOOP_PATH}/clustering-chi2-output/top-centroid-id-{i}",
                ],
            )[0]
            .replace("\n", "")
            .split("\t")
        )
        print("Connected the furthest distance centroid id with its ratings")

        # Remove highest centroid
        run_mr_job_hadoop(
            RemoveCentroid,
            [
                f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i}",
                "--centroid",
                max_centroid[0],
            ],
            f"{HADOOP_PATH}/clustering-chi2-output/centroids-{i + 1}",
            True,
        )
        print("Removed highest centroid")

    # Filter only centroids
    centroids = run_mr_job_hadoop(
        FilterCentroids,
        [
            f"{HADOOP_PATH}/clustering-chi2-output/centroids-{number_of_clusters - 1}",
        ],
        f"{HADOOP_PATH}/clustering-chi2-output/centroids-{number_of_clusters}",
        True,
    )
    print("Filtered only centroids")

    # Run Kmeans
    return kmeans(number_of_clusters, "clustering-chi2-output", centroids)
