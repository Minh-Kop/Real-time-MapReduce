import os
import sys

sys.path.append(os.path.abspath("./clustering/util"))

import pandas as pd

from custom_util import run_mr_job, write_data_to_file
from .calculate_avg_rating import AvgRating
from .create_user_item_matrix import UserItemMatrix
from .create_importance import Importance
from .get_max import get_max, GetMax
from .create_centroid import CreateCentroid
from .create_centroids_list import CreateCentroidsList
from .calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from .calculate_M_nearest_points import M_nearest_points_pandas, MNearestPoints
from .discard_nearest_points import DiscardNearestPoints
from .calculate_scaling import Scaling
from .calculate_sum_F_D import SumFD
from .update_centroids import UpdateCentroids
from .label import Label


def create_centroids_list(num):
    run_mr_job(
        CreateCentroidsList,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/centroid-*",
        ],
        f"hdfs://localhost:9000/user/mackop/clustering-output/centroids-{num}",
        True,
    )


def run_clustering(input_file_path, number_of_clusters=3):
    # Calculate average rating
    run_mr_job(
        AvgRating,
        [input_file_path],
        "hdfs://localhost:9000/user/mackop/clustering-output/avg",
    )

    # Create user-item matrix
    run_mr_job(
        UserItemMatrix,
        [
            input_file_path,
            "hdfs://localhost:9000/user/clustering-output/output/avg",
            "--items-path",
            "./input/items.txt",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/matrix",
    )

    # Calculate importance
    run_mr_job(
        Importance,
        [input_file_path],
        "hdfs://localhost:9000/user/mackop/clustering-output/F",
    )

    # Find most importance
    run_mr_job(
        GetMax,
        ["hdfs://localhost:9000/user/mackop/clustering-output/F"],
        "hdfs://localhost:9000/user/mackop/clustering-output/max-F",
    )

    # Create first centroid
    run_mr_job(
        CreateCentroid,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/matrix",
            "hdfs://localhost:9000/user/mackop/clustering-output/max-F",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/centroid",
    )
    create_centroids_list(0)

    # Calculate number of discarded points
    users_file = open("./input/users.txt", "r")
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()
    M = int(number_of_users / 4 / 1.5) + 1

    # Calculate distance between users and first centroid
    run_mr_job(
        DistanceBetweenUsersCentroid,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/matrix",
            "--first-centroid-path",
            "hdfs://localhost:9000/user/mackop/input/centroids-0.txt",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/D",
    )

    # Calculate M nearest points
    run_mr_job(
        MNearestPoints,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/D",
            "--M",
            str(M),
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points",
    )

    # Discard nearest points in user-item matrix
    run_mr_job(
        DiscardNearestPoints,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/matrix",
            "hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/matrix-0",
    )

    # Discard nearest points in F
    run_mr_job(
        DiscardNearestPoints,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/F",
            "hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/F-0",
    )

    # Loop
    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        run_mr_job(
            DistanceBetweenUsersCentroid,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/matrix-{i}",
                "--first-centroid-path",
                f"hdfs://localhost:9000/user/mackop/input/centroids-{i}.txt",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/D-{i}",
        )

        # Get max F
        run_mr_job(
            GetMax,
            [f"hdfs://localhost:9000/user/mackop/clustering-output/F-{i}"],
            f"hdfs://localhost:9000/user/mackop/clustering-output/max-F-{i}",
        )

        # Scaling F
        result_data = run_mr_job(
            Scaling,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/F-{i}"
                "--max-value-path",
                f"hdfs://localhost:9000/user/mackop/clustering-output/max-F-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/scaling-F-{i}",
        )

        # Get max min_D
        run_mr_job(
            GetMax,
            [f"hdfs://localhost:9000/user/mackop/clustering-output/D-{i}"],
            f"hdfs://localhost:9000/user/mackop/clustering-output/max-D-{i}",
        )

        # Scaling D
        run_mr_job(
            Scaling,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/D-{i}",
                "--max-value-path",
                f"hdfs://localhost:9000/user/mackop/clustering-output/max-D-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/scaling-D-{i}",
        )

        # Calculate sum F, D
        result_data = run_mr_job(
            SumFD,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/scaling-F-{i}",
                f"hdfs://localhost:9000/user/mackop/clustering-output/scaling-D-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/F-D-{i}",
        )

        # Calculate max F_D
        run_mr_job(
            GetMax,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/F-D-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/max-F-D-{i}",
        )

        # Create another centroid
        run_mr_job(
            CreateCentroid,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/matrix-{i}",
                f"hdfs://localhost:9000/user/mackop/clustering-output/max-F-D-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/centroid-{i}",
            True,
        )
        create_centroids_list(i + 1)

        # Calculate distance between new centroid and other users
        result_data = run_mr_job(
            DistanceBetweenUsersCentroid,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/matrix-{i}",
                "--first-centroid-path",
                f"hdfs://localhost:9000/user/mackop/input/centroid-{i}.txt",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/D_-{i}",
        )

        # Calculate M nearest points
        run_mr_job(
            MNearestPoints,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/D_-{i}",
                "--M",
                str(M),
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points-{i}",
        )

        # Discard nearest points in user-item matrix
        result_data = run_mr_job(
            DiscardNearestPoints,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/matrix-{i}",
                f"hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/matrix-{i+1}",
        )
        if result_data == []:
            print("Break")
            break

        # Discard nearest points in F
        run_mr_job(
            DiscardNearestPoints,
            [
                f"hdfs://localhost:9000/user/mackop/clustering-output/F-{i}",
                f"hdfs://localhost:9000/user/mackop/clustering-output/M-nearest-points-{i}",
            ],
            f"hdfs://localhost:9000/user/mackop/clustering-output/F-{i+1}",
        )

    # KMeans
    count = 1
    while True:
        print(f"\nLoop {count}")
        count += 1

        # Calculate distance between users and centroids
        result_data = run_mr_job(
            DistanceBetweenUsersCentroid,
            [
                "./clustering/output/user_item_matrix_.txt",
                "--first-centroid-path",
                "./clustering/output/centroids.txt",
                "--return-centroid-id",
                "True",
            ],
        )
        write_data_to_file("./clustering/output/user_item_matrix.txt", result_data)

        # Update centroids
        result_data = run_mr_job(
            UpdateCentroids, ["./clustering/output/user_item_matrix.txt"]
        )
        write_data_to_file("./clustering/output/new_centroids.txt", result_data)

        # Check if has converged
        with open("./clustering/output/new_centroids.txt", "r") as new_centroids, open(
            "./clustering/output/centroids.txt", "r"
        ) as old_centroids:
            new_centroids_tuples = []
            old_centroids_tuples = []
            for line in new_centroids:
                key, value = line.strip().split("\t")
                new_centroids_tuples.append(tuple(value.strip().split("|")))
            for line in old_centroids:
                key, value = line.strip().split("\t")
                old_centroids_tuples.append(tuple(value.strip().split("|")))

            new_centroids_tuples = tuple(new_centroids_tuples)
            old_centroids_tuples = tuple(old_centroids_tuples)
            if set(new_centroids_tuples) == set(old_centroids_tuples):
                break

        # Save new centroids to file
        with open("./clustering/output/new_centroids.txt", "r") as new_centroids, open(
            "./clustering/output/centroids.txt", "w"
        ) as old_centroids:
            for line in new_centroids:
                old_centroids.write(line)

    # Assign labels
    run_mr_job(
        Label,
        [
            "hdfs://localhost:9000/user/mackop/clustering-output/matrix",
        ],
        "hdfs://localhost:9000/user/mackop/clustering-output/labels",
    )


if __name__ == "__main__":
    # Create users, items files
    input_file_path = pd.read_csv(
        "./input/input_file_path copy.txt",
        sep="\t",
        names=["key", "values"],
        dtype="str",
        usecols=["key"],
    )
    input_file_path = input_file_path["key"].str.split(";", expand=True)
    users = input_file_path[0]
    items = input_file_path[1]

    users.drop_duplicates().to_csv("./input/users.txt", index=False, header=False)
    items.drop_duplicates().to_csv("./input/items.txt", index=False, header=False)

    run_clustering("./input/input_file_path copy.txt")
