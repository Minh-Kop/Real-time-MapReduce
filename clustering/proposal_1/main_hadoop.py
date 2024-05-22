import os
import sys
import re

sys.path.append(os.path.abspath("./clustering/util"))
sys.path.append(os.path.abspath("./clustering/common"))

from custom_util import run_mr_job_hadoop, env_dict
from create_user_item_matrix import UserItemMatrix
from calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from kmeans import kmeans
from get_max import GetMax
from create_centroids import CreateCentroids
from calculate_M_nearest_points import MNearestPoints
from .calculate_avg_rating import AvgRating
from .create_importance import Importance
from .create_centroids_list import CreateCentroidsList
from .discard_nearest_points import DiscardNearestPoints
from .calculate_scaling import Scaling
from .calculate_sum_F_D import SumFD

HADOOP_PATH = env_dict["hadoop_path"]


def create_centroids_list(num):
    input = [f"{HADOOP_PATH}/clustering-output/centroid-{i}" for i in range(num + 1)]

    return run_mr_job_hadoop(
        CreateCentroidsList,
        input,
        f"{HADOOP_PATH}/clustering-output/centroids-{num}",
        True,
    )


def get_max_value(values):
    return float(re.search(r"\t(.*?)\n", values[0]).group(1))


def run_clustering(input_file_path, number_of_clusters=3):
    # Calculate average rating
    run_mr_job_hadoop(
        AvgRating,
        [input_file_path],
        f"{HADOOP_PATH}/clustering-output/avg-ratings",
        True,
    )
    print("Calculate average rating")

    # Create user-item matrix
    run_mr_job_hadoop(
        UserItemMatrix,
        [
            input_file_path,
            f"{HADOOP_PATH}/clustering-output/avg-ratings",
            "--items-path",
            f"{HADOOP_PATH}/input/items.txt",
        ],
        f"{HADOOP_PATH}/clustering-output/full-matrix",
    )
    print("Create user-item matrix")

    # Calculate importance
    run_mr_job_hadoop(
        Importance,
        [input_file_path],
        f"{HADOOP_PATH}/clustering-output/F",
    )
    print("Calculate importance")

    # Find most importance
    run_mr_job_hadoop(
        GetMax,
        [f"{HADOOP_PATH}/clustering-output/F"],
        f"{HADOOP_PATH}/clustering-output/max-F",
    )
    print("Find most importance")

    # Create first centroid
    run_mr_job_hadoop(
        CreateCentroids,
        [
            f"{HADOOP_PATH}/clustering-output/full-matrix",
            f"{HADOOP_PATH}/clustering-output/max-F",
        ],
        f"{HADOOP_PATH}/clustering-output/centroid-0",
    )
    create_centroids_list(0)
    print("Create first centroid")

    # Calculate number of discarded points
    users_file = open("./input/users.txt", "r")
    for number_of_users, line in enumerate(users_file, start=1):
        pass
    users_file.close()
    M = int(number_of_users / 4 / 1.5) + 1
    print("Calculate number of discarded points")

    # Calculate distance between users and first centroid
    run_mr_job_hadoop(
        DistanceBetweenUsersCentroid,
        [
            f"{HADOOP_PATH}/clustering-output/full-matrix",
            "--centroids-path",
            f"{HADOOP_PATH}/input/centroids-0.txt",
        ],
        f"{HADOOP_PATH}/clustering-output/D",
    )
    print("Calculate distance between users and first centroid")

    # Calculate M nearest points
    run_mr_job_hadoop(
        MNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-output/D",
            "--M",
            str(M),
        ],
        f"{HADOOP_PATH}/clustering-output/M-nearest-points",
    )
    print("Calculate M nearest points")

    # Discard nearest points in user-item matrix
    run_mr_job_hadoop(
        DiscardNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-output/full-matrix",
            f"{HADOOP_PATH}/clustering-output/M-nearest-points",
        ],
        f"{HADOOP_PATH}/clustering-output/matrix-0",
    )
    print("Discard nearest points in user-item matrix")

    # Discard nearest points in F
    run_mr_job_hadoop(
        DiscardNearestPoints,
        [
            f"{HADOOP_PATH}/clustering-output/F",
            f"{HADOOP_PATH}/clustering-output/M-nearest-points",
        ],
        f"{HADOOP_PATH}/clustering-output/F-0",
    )
    print("Discard nearest points in F")

    # Loop
    for i in range(number_of_clusters - 1):
        print(i)

        # Calculate distance between users and centroids
        run_mr_job_hadoop(
            DistanceBetweenUsersCentroid,
            [
                f"{HADOOP_PATH}/clustering-output/matrix-{i}",
                "--centroids-path",
                f"{HADOOP_PATH}/input/centroids-{i}.txt",
            ],
            f"{HADOOP_PATH}/clustering-output/D-{i}",
        )
        print("Calculate distance between users and centroids")

        # Get max F
        return_value = run_mr_job_hadoop(
            GetMax, [f"{HADOOP_PATH}/clustering-output/F-{i}"], None
        )
        F_max = get_max_value(return_value)
        print("Get max F")

        # Get scaling F
        run_mr_job_hadoop(
            Scaling,
            [
                f"{HADOOP_PATH}/clustering-output/F-{i}",
                "--max-value",
                str(F_max),
            ],
            f"{HADOOP_PATH}/clustering-output/scaling-F-{i}",
        )
        print("Get scaling F")

        # Get max min_D
        return_value = run_mr_job_hadoop(
            GetMax, [f"{HADOOP_PATH}/clustering-output/D-{i}"], None
        )
        D_max = get_max_value(return_value)
        print("Get max min_D")

        # Get scaling D
        run_mr_job_hadoop(
            Scaling,
            [
                f"{HADOOP_PATH}/clustering-output/D-{i}",
                "--max-value",
                str(D_max),
            ],
            f"{HADOOP_PATH}/clustering-output/scaling-D-{i}",
        )
        print("Get scaling D")

        # Calculate sum scaling F, scaling D
        result_data = run_mr_job_hadoop(
            SumFD,
            [
                f"{HADOOP_PATH}/clustering-output/scaling-F-{i}",
                f"{HADOOP_PATH}/clustering-output/scaling-D-{i}",
            ],
            f"{HADOOP_PATH}/clustering-output/F-D-{i}",
        )
        print("Calculate sum scaling F, scaling D")

        # Calculate max F_D
        run_mr_job_hadoop(
            GetMax,
            [
                f"{HADOOP_PATH}/clustering-output/F-D-{i}",
            ],
            f"{HADOOP_PATH}/clustering-output/max-F-D-{i}",
        )
        print("Calculate max F_D")

        # Create another centroid
        run_mr_job_hadoop(
            CreateCentroids,
            [
                f"{HADOOP_PATH}/clustering-output/matrix-{i}",
                f"{HADOOP_PATH}/clustering-output/max-F-D-{i}",
            ],
            f"{HADOOP_PATH}/clustering-output/centroid-{i + 1}",
            True,
        )
        centroids = create_centroids_list(i + 1)
        print("Create another centroid")

        # Calculate distance between new centroid and other users
        result_data = run_mr_job_hadoop(
            DistanceBetweenUsersCentroid,
            [
                f"{HADOOP_PATH}/clustering-output/matrix-{i}",
                "--centroids-path",
                f"{HADOOP_PATH}/input/centroid-{i + 1}.txt",
            ],
            f"{HADOOP_PATH}/clustering-output/D_-{i}",
        )
        print("Calculate distance between new centroid and other users")

        # Calculate M nearest points
        run_mr_job_hadoop(
            MNearestPoints,
            [
                f"{HADOOP_PATH}/clustering-output/D_-{i}",
                "--M",
                str(M),
            ],
            f"{HADOOP_PATH}/clustering-output/M-nearest-points-{i}",
        )
        print("Calculate M nearest points")

        # Discard nearest points in user-item matrix
        result_data = run_mr_job_hadoop(
            DiscardNearestPoints,
            [
                f"{HADOOP_PATH}/clustering-output/matrix-{i}",
                f"{HADOOP_PATH}/clustering-output/M-nearest-points-{i}",
            ],
            f"{HADOOP_PATH}/clustering-output/matrix-{i+1}",
        )
        print("Discard nearest points in user-item matrix")
        if result_data == []:
            print("Break")
            break

        # Discard nearest points in F
        run_mr_job_hadoop(
            DiscardNearestPoints,
            [
                f"{HADOOP_PATH}/clustering-output/F-{i}",
                f"{HADOOP_PATH}/clustering-output/M-nearest-points-{i}",
            ],
            f"{HADOOP_PATH}/clustering-output/F-{i+1}",
        )
        print("Discard nearest points in F")

    # KMeans
    return kmeans(number_of_clusters - 1, "clustering-output", centroids)


if __name__ == "__main__":
    input_file_path = "./input/input_file_copy.txt"
    run_clustering(input_file_path, number_of_clusters=3)
