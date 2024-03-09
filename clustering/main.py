import os
import sys

sys.path.append(os.path.abspath("./clustering/util"))

import pandas as pd

from custom_util import run_mr_job, write_data_to_file
from .calculate_avg_rating import AvgRating
from .create_user_item_matrix import UserItemMatrix
from .create_importance import Importance
from .get_max import get_max
from .create_centroid import CreateCentroid
from .calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from .calculate_M_nearest_points import M_nearest_points_pandas
from .discard_nearest_points import DiscardNearestPoints
from .calculate_scaling import Scaling
from .calculate_sum_F_D import SumFD
from .update_centroids import UpdateCentroids
from .label import Label


def run_clustering(input_file, number_of_clusters=3):
    # Calculate average rating
    result_data = run_mr_job(AvgRating, [input_file])
    write_data_to_file("./clustering/output/avg_ratings.txt", result_data)

    # Create user-item matrix
    result_data = run_mr_job(
        UserItemMatrix,
        [
            input_file,
            "./clustering/output/avg_ratings.txt",
            "--items-path",
            "./input/items.txt",
        ],
    )
    write_data_to_file("./clustering/output/user_item_matrix.txt", result_data)
    write_data_to_file("./clustering/output/user_item_matrix_.txt", result_data)

    # Calculate importance
    result_data = run_mr_job(Importance, [input_file])
    write_data_to_file("./clustering/output/F.txt", result_data)

    # Find most importance
    get_max("./clustering/output/F.txt", "./clustering/output/max_F.txt")

    # Create first centroid
    result_data = run_mr_job(
        CreateCentroid,
        [
            "./clustering/output/user_item_matrix.txt",
            "./clustering/output/max_F.txt",
        ],
    )
    write_data_to_file("./clustering/output/centroids.txt", result_data)

    # Calculate number of discarded points
    users_file = open("./input/users.txt", "r")
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()
    M = int(number_of_users / 4 / 1.5) + 1

    # Calculate distance between users and first centroid
    result_data = run_mr_job(
        DistanceBetweenUsersCentroid,
        [
            "./clustering/output/user_item_matrix.txt",
            "--first-centroid-path",
            "./clustering/output/centroids.txt",
        ],
    )
    write_data_to_file("./clustering/output/D.txt", result_data)

    # Calculate M nearest points
    M_nearest_points_pandas(
        "./clustering/output/D.txt", M, "./clustering/output/M_nearest_points.txt"
    )

    # Discard nearest points in user-item matrix
    result_data = run_mr_job(
        DiscardNearestPoints,
        [
            "./clustering/output/user_item_matrix.txt",
            "./clustering/output/M_nearest_points.txt",
        ],
    )
    write_data_to_file("./clustering/output/user_item_matrix.txt", result_data)

    # Discard nearest points in F
    result_data = run_mr_job(
        DiscardNearestPoints,
        ["./clustering/output/F.txt", "./clustering/output/M_nearest_points.txt"],
    )
    write_data_to_file("./clustering/output/F.txt", result_data)

    # Loop
    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        result_data = run_mr_job(
            DistanceBetweenUsersCentroid,
            [
                "./clustering/output/user_item_matrix.txt",
                "--first-centroid-path",
                "./clustering/output/centroids.txt",
            ],
        )
        write_data_to_file("./clustering/output/D.txt", result_data)

        # Get max F
        get_max("./clustering/output/F.txt", "./clustering/output/max_F.txt")

        # Scaling F
        result_data = run_mr_job(
            Scaling,
            [
                "./clustering/output/F.txt",
                "--max-value-path",
                "./clustering/output/max_F.txt",
            ],
        )
        write_data_to_file("./clustering/output/F.txt", result_data)

        # Get max min_D
        get_max("./clustering/output/D.txt", "./clustering/output/max_D.txt")

        # Scaling D
        result_data = run_mr_job(
            Scaling,
            [
                "./clustering/output/D.txt",
                "--max-value-path",
                "./clustering/output/max_D.txt",
            ],
        )
        write_data_to_file("./clustering/output/D.txt", result_data)

        # Calculate sum F, D
        result_data = run_mr_job(
            SumFD, ["./clustering/output/F.txt", "./clustering/output/D.txt"]
        )
        write_data_to_file("./clustering/output/F_D.txt", result_data)

        # Calculate max F_D
        get_max("./clustering/output/F_D.txt", "./clustering/output/max_F_D.txt")

        # Create another centroid
        result_data = run_mr_job(
            CreateCentroid,
            [
                "./clustering/output/user_item_matrix.txt",
                "./clustering/output/max_F_D.txt",
            ],
        )
        write_data_to_file("./clustering/output/new_centroid.txt", result_data)
        write_data_to_file("./clustering/output/centroids.txt", result_data, mode="a")

        # Calculate distance between new centroid and other users
        result_data = run_mr_job(
            DistanceBetweenUsersCentroid,
            [
                "./clustering/output/user_item_matrix.txt",
                "--first-centroid-path",
                "./clustering/output/new_centroid.txt",
            ],
        )
        write_data_to_file("./clustering/output/D_.txt", result_data)

        # Calculate M nearest points
        M_nearest_points_pandas(
            "./clustering/output/D_.txt", M, "./clustering/output/M_nearest_points.txt"
        )

        # Discard nearest points in user-item matrix
        result_data = run_mr_job(
            DiscardNearestPoints,
            [
                "./clustering/output/user_item_matrix.txt",
                "./clustering/output/M_nearest_points.txt",
            ],
        )
        if result_data == []:
            print("Break")
            break
        write_data_to_file("./clustering/output/user_item_matrix.txt", result_data)

        # Discard nearest points in F
        result_data = run_mr_job(
            DiscardNearestPoints,
            [
                "./clustering/output/F.txt",
                "./clustering/output/M_nearest_points.txt",
            ],
        )
        write_data_to_file("./clustering/output/F.txt", result_data)

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
    result_data = run_mr_job(Label, ["./clustering/output/user_item_matrix.txt"])
    write_data_to_file("./clustering/output/labels.txt", result_data)


if __name__ == "__main__":
    # Create users, items files
    input_file = pd.read_csv(
        "./input/input_file copy.txt",
        sep="\t",
        names=["key", "values"],
        dtype="str",
        usecols=["key"],
    )
    input_file = input_file["key"].str.split(";", expand=True)
    users = input_file[0]
    items = input_file[1]

    users.drop_duplicates().to_csv("./input/users.txt", index=False, header=False)
    items.drop_duplicates().to_csv("./input/items.txt", index=False, header=False)

    run_clustering("./input/input_file copy.txt")
