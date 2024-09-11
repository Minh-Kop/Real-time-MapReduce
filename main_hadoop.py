import os
import sys
import time

sys.path.append(os.path.abspath("./util"))

import pandas as pd

from custom_util import (
    env_dict,
    write_data_to_file,
    get_txt_filename,
    put_files_to_hdfs,
    create_and_delete_intermediate_directories,
)

# from clustering import run_proposal_1_clustering
# from clustering import run_clustering_proposal_2_chi2
from clustering import run_clustering_proposal_2_chi2_ext1
from mfps.main_hadoop import run_mfps

HADOOP_PATH = env_dict["hadoop_path"]
NUMBER_OF_CLUSTERS = 3
MULTIPLIER = 10


def create_input_file(input_path, output_path):
    df = pd.read_csv(
        input_path,
        sep="\t",
        names=["user_id", "item_id", "rating", "timestamp"],
        dtype="str",
    )
    input_df = pd.DataFrame(
        {
            "key": df["user_id"] + ";" + df["item_id"],
            "value": df["rating"] + ";" + df["timestamp"],
        }
    )
    input_df.to_csv(output_path, sep="\t", index=False, header=False)


def split_files_by_label(input_file_path, num):
    input_file = pd.read_csv(
        input_file_path, sep="\t", dtype="str", names=["key", "value"]
    )
    input_file["user"] = input_file["key"].str.split(";", expand=True)[0]

    labels = pd.read_csv(
        "./hadoop_output/labels.txt",
        sep="\t",
        dtype="str",
        names=["user", "label"],
    )

    joined_df = pd.merge(input_file, labels, on="user").drop(columns="user")
    joined_df = joined_df.set_index("label")

    avg_ratings = pd.read_csv(
        "./hadoop_output/avg-ratings.txt",
        sep="\t",
        dtype="str",
        names=["user", "avg_rating"],
    )
    avg_ratings = avg_ratings.merge(labels, on="user").set_index("label")

    centroids = pd.read_csv(
        f"./hadoop_output/centroids-{num}.txt",
        sep="\t",
        dtype="str",
        names=["key", "value"],
    )
    for index, centroid in enumerate(centroids["key"]):
        # Export input file
        input_file_i = joined_df.loc[[centroid]]
        input_file_i_path = f"input/input-file-{index}.txt"
        input_file_i.to_csv(input_file_i_path, sep="\t", index=False, header=False)
        put_files_to_hdfs(input_file_i_path, f"{input_file_i_path}")

        # Export average ratings
        avg_ratings_i = avg_ratings.loc[[centroid]]
        avg_ratings_i_path = f"input/avg-ratings-{index}.txt"
        avg_ratings_i.to_csv(avg_ratings_i_path, sep="\t", index=False, header=False)
        put_files_to_hdfs(avg_ratings_i_path, f"{avg_ratings_i_path}")

        # Update centroid key
        centroids.loc[index, "key"] = index

    # Export new centroids
    centroids_path = "input/centroids.txt"
    centroids.to_csv(centroids_path, sep="\t", index=False, header=False)
    put_files_to_hdfs(centroids_path, centroids_path)


if __name__ == "__main__":
    # input_file_path = "input/input_file_1M.txt"
    # item_file_path = "input/items_1M.txt"

    # input_file_path = "./input/input_file_10M.txt"
    # item_file_path = "input/items_10M.txt"

    # input_file_path = "./input/input_file_100k.txt"
    # item_file_path = "input/items_100k.txt"

    input_file_path = "./input/input_file_t.txt"
    item_file_path = "input/items_t.txt"

    hdfs_input_file_path = f"{HADOOP_PATH}/input/{get_txt_filename(input_file_path)}"
    hdfs_item_file_path = f"{HADOOP_PATH}/input/{get_txt_filename(item_file_path)}"

    ## Put input files to HDFS
    put_files_to_hdfs(input_file_path, hdfs_input_file_path)
    put_files_to_hdfs(item_file_path, hdfs_item_file_path)

    ## Start clustering timer
    start_time = time.perf_counter()

    ## Clustering
    num = create_and_delete_intermediate_directories(
        lambda: run_clustering_proposal_2_chi2_ext1(
            hdfs_input_file_path,
            item_file_path,
            hdfs_item_file_path,
            NUMBER_OF_CLUSTERS,
            number_of_multiplications=MULTIPLIER,
        ),
        "clustering-chi2-output",
    )

    ## End clustering timer
    end_time = time.perf_counter()

    ## Calculate clustering elapsed time
    print(f"\nRunning clustering time: {end_time - start_time}s")
    print("\n=============================================================\n")

    ## Split input file
    split_files_by_label(input_file_path, num=num)

    ## Start MFPS timer
    start_time = time.perf_counter()

    ## Calculate MFPS
    mfps_result = []
    for index in range(NUMBER_OF_CLUSTERS):
        print(f"\nLoop MFPS {index}")
        input_path = f"{HADOOP_PATH}/input/input-file-{index}.txt"
        avg_ratings_path = f"{HADOOP_PATH}/input/avg-ratings-{index}.txt"
        output_path = f"{HADOOP_PATH}/final-output/mfps-{index}"

        result_data = create_and_delete_intermediate_directories(
            lambda: run_mfps(
                input_path=input_path,
                avg_ratings_path=avg_ratings_path,
                output_path=output_path,
            ),
            "mfps-output",
        )
        mfps_result.append(result_data)

    mfps_result = [line for row in mfps_result for line in row]

    # output_path = f"./hadoop_output/mfps_1m.txt"k
    output_path = f"./hadoop_output/mfps_100k.txt"
    write_data_to_file(output_path, mfps_result)

    ## End MFPS timer
    end_time = time.perf_counter()

    ## Calculate MFPS elapsed time
    print(f"\nRunning MFPS time: {end_time - start_time}s")
