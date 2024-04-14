import os
import sys

sys.path.append(os.path.abspath("./util"))

import pandas as pd

from custom_util import write_data_to_file, get_txt_filename, put_files_to_hdfs
from clustering.main_hadoop import run_clustering
from mfps.main_hadoop import run_mfps


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


def create_users_items_file(input_path):
    input_df = pd.read_csv(input_path, sep="\t", names=["key", "value"])
    input_df = input_df["key"].str.split(";", expand=True)

    # Drop duplicates and sort
    users = input_df[0].astype("int64").drop_duplicates().sort_values()
    items = input_df[1].astype("int64").drop_duplicates().sort_values()

    # Export to csv
    users.to_csv("./input/users.txt", index=False, header=False)
    items.to_csv("./input/items.txt", index=False, header=False)

    put_files_to_hdfs("./input/items.txt", "/user/mackop/input/items.txt")


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
    labels["label"] = labels["label"].str.split("|", expand=True)[0]

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
        names=["key"],
        usecols=[0],
    )
    for index, centroid in enumerate(centroids["key"]):
        # Export input file
        input_file_i = joined_df.loc[[centroid]]
        input_file_i_path = f"input/input-file-{index}.txt"
        input_file_i.to_csv(input_file_i_path, sep="\t", index=False, header=False)
        put_files_to_hdfs(input_file_i_path, f"/user/mackop/{input_file_i_path}")

        # Export average ratings
        avg_ratings_i = avg_ratings.loc[[centroid]]
        avg_ratings_i_path = f"input/avg-ratings-{index}.txt"
        avg_ratings_i.to_csv(avg_ratings_i_path, sep="\t", index=False, header=False)
        put_files_to_hdfs(avg_ratings_i_path, f"/user/mackop/{avg_ratings_i_path}")


if __name__ == "__main__":
    HDFS_PATH = "hdfs://localhost:9000/user/mackop"
    NUMBER_OF_CLUSTERS = 3

    source_file_path = "./input/u.data"
    input_file_path = "./input/input_file_copy.txt"
    hdfs_input_file_path = f"{HDFS_PATH}/input/{get_txt_filename(input_file_path)}"

    # create_input_file(input_path=source_file_path, output_path=input_file_path)
    put_files_to_hdfs(input_file_path, hdfs_input_file_path)

    create_users_items_file(input_file_path)

    # Clustering
    num = run_clustering(input_file_path, NUMBER_OF_CLUSTERS)

    # Split input file
    split_files_by_label(input_file_path, num=num)

    # MFPS
    mfps_result = []
    for index in range(NUMBER_OF_CLUSTERS):

        input_path = f"{HDFS_PATH}/input/input-file-{index}.txt"
        avg_ratings_path = f"{HDFS_PATH}/input/avg-ratings-{index}.txt"
        output_path = f"{HDFS_PATH}/mfps-output/mfps-{index}"

        result_data = run_mfps(
            input_path=input_path,
            avg_ratings_path=avg_ratings_path,
            output_path=output_path,
        )
        mfps_result.append(result_data)
    mfps_result = [line for row in mfps_result for line in row]

    output_path = f"./hadoop_output/mfps.txt"
    write_data_to_file(output_path, mfps_result)