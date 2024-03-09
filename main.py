import os
import sys

sys.path.append(os.path.abspath("./util"))

import pandas as pd

from custom_util import write_data_to_file
from clustering import run_clustering
from mfps import run_mfps


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

    users = input_df[0].astype("int64").drop_duplicates().sort_values()
    items = input_df[1].astype("int64").drop_duplicates().sort_values()

    users.to_csv("./input/users.txt", index=False, header=False)
    items.to_csv("./input/items.txt", index=False, header=False)


def split_files_by_label(input_file_path):
    input_file = pd.read_csv(
        input_file_path, sep="\t", dtype="str", names=["key", "value"]
    )
    input_file["user"] = input_file["key"].str.split(";", expand=True)[0]

    labels = pd.read_csv(
        "./clustering/output/labels.txt", sep="\t", dtype="str", names=["user", "label"]
    )
    labels["label"] = labels["label"].str.split("|", expand=True)[0]

    joined_df = pd.merge(input_file, labels, on="user").drop(columns="user")
    joined_df = joined_df.set_index("label")

    avg_ratings = pd.read_csv(
        "./clustering/output/avg_ratings.txt",
        sep="\t",
        dtype="str",
        names=["user", "avg_rating"],
    )
    avg_ratings = avg_ratings.merge(labels, on="user").set_index("label")

    centroids = pd.read_csv(
        "./clustering/output/centroids.txt",
        sep="\t",
        dtype="str",
        names=["key"],
        usecols=[0],
    )
    for index, value in enumerate(centroids["key"]):
        input_file_i = joined_df.loc[[value]]

        # Export input file
        input_file_i.to_csv(
            (f"./input/input_file_{index}.txt"), sep="\t", index=False, header=False
        )

        avg_ratings_i = avg_ratings.loc[[value]]

        # Export average ratings
        avg_ratings_i.to_csv(
            (f"./input/avg_ratings_{index}.txt"), sep="\t", index=False, header=False
        )


if __name__ == "__main__":
    source_file_path = "./input/u.data"
    input_file_path = "./input/input_file copy.txt"
    NUMBER_OF_CLUSTERS = 3
    # create_input_file(input_path=source_file_path, output_path=input_file_path)
    create_users_items_file(input_file_path)

    # Clustering
    run_clustering(input_file_path, NUMBER_OF_CLUSTERS)

    # Split input file
    split_files_by_label(input_file_path)

    # MFPS
    mfps_result = []
    for index in range(NUMBER_OF_CLUSTERS):
        input_path = f"./input/input_file_{index}.txt"
        avg_ratings_path = f"./input/avg_ratings_{index}.txt"
        output_path = f"./output/mfps_{index}.txt"

        result_data = run_mfps(
            input_path=input_path,
            avg_ratings_path=avg_ratings_path,
            output_path=output_path,
        )
        mfps_result.append(result_data)
    mfps_result = [line for row in mfps_result for line in row]

    output_path = f"./output/mfps.txt"
    write_data_to_file(output_path, mfps_result)
