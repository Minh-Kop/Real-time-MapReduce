import os
import sys

sys.path.append(os.path.abspath("./clustering/util"))

from custom_util import run_mr_job_hadoop, env_dict
from calculate_distance_between_users_centroid import DistanceBetweenUsersCentroid
from update_centroids import UpdateCentroids
from label import Label

HADOOP_PATH = env_dict["hadoop_path"]


def kmeans(i, clustering_folder, centroids):
    count = 0
    while True:
        print(f"\nLoop K-means {count}")

        # Calculate distance between users and centroids
        run_mr_job_hadoop(
            DistanceBetweenUsersCentroid,
            [
                f"{HADOOP_PATH}/{clustering_folder}/matrix",
                "--centroids-path",
                f"{HADOOP_PATH}/temp-input/centroids-{i}.txt",
                "--return-centroid-id",
                "True",
            ],
            f"{HADOOP_PATH}/{clustering_folder}/matrix-{count}",
            True,
        )
        print("Calculated distance between users and centroids")

        # Update centroids
        updated_centroids = run_mr_job_hadoop(
            UpdateCentroids,
            [f"{HADOOP_PATH}/{clustering_folder}/matrix-{count}"],
            f"{HADOOP_PATH}/{clustering_folder}/centroids-{i+1}",
            True,
        )
        print("Updated centroids")

        # Check if has converged
        updated_centroids_tuples = []
        centroids_tuples = []
        for line in updated_centroids:
            _, value = line.strip().split("\t")
            updated_centroids_tuples.append(tuple(value.strip().split("|")))
        for line in centroids:
            _, value = line.strip().split("\t")
            centroids_tuples.append(tuple(value.strip().split("|")))

        updated_centroids_tuples = tuple(updated_centroids_tuples)
        centroids_tuples = tuple(centroids_tuples)
        if set(updated_centroids_tuples) == set(centroids_tuples):
            print("\nConverged\n")
            break

        centroids = updated_centroids
        count += 1
        i += 1

    # Assign labels
    run_mr_job_hadoop(
        Label,
        [f"{HADOOP_PATH}/{clustering_folder}/matrix-{count}"],
        f"{HADOOP_PATH}/{clustering_folder}/labels",
        True,
    )
    print("Assigned labels")

    return i
