import os
import sys

sys.path.append(os.path.abspath("./util"))

import pandas as pd

from custom_util import (
    env_dict,
    write_data_to_file,
    get_txt_filename,
    put_files_to_hdfs,
)
from clustering import run_proposal_1_clustering
from mfps.main_hadoop import run_mfps
from clustering import run_clustering_proposal_2_chi2_ext1
from clustering import run_clustering_proposal_2_chi2
from eval_function import split_test_train, evaluate


HADOOP_PATH = env_dict["hadoop_path"]
NUMBER_OF_CLUSTERS = 2
MULTIPLIER = 1


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
    # items.to_csv("./input/items.txt", index=False, header=False)

    put_files_to_hdfs("./input/items.txt", "input/items.txt")


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
        put_files_to_hdfs(input_file_i_path, f"{input_file_i_path}")

        # Export average ratings
        avg_ratings_i = avg_ratings.loc[[centroid]]
        avg_ratings_i_path = f"input/avg-ratings-{index}.txt"
        avg_ratings_i.to_csv(avg_ratings_i_path, sep="\t", index=False, header=False)
        put_files_to_hdfs(avg_ratings_i_path, f"{avg_ratings_i_path}")


def create_item_file(item_path):
    item_df = pd.read_csv(item_path, sep="|", names=[str(i) for i in range(24)])
    item_df = item_df.astype(str)
    item_df["genre"] = item_df[[str(i) for i in range(5, 24)]].agg("|".join, axis=1)
    item_df = item_df.drop([str(i) for i in range(1, 24)], axis=1)

    item_df.to_csv("./input/items.txt", sep="\t", index=False, header=False)


if __name__ == "__main__":
    source_file_path = "./input/u.data"
    item_file_path = "./input/u.item"
    all_user_path = "./input/input_file_copy.txt"
    test_file_path = "./input/test_input.txt"
    train_file_path = "./input/train_input.txt"
    hdfs_input_file_path = f"{HADOOP_PATH}/input/{get_txt_filename(train_file_path)}"

    # create_item_file(item_file_path)

    # create_input_file(input_path=source_file_path, output_path=all_user_path)
    # split_test_train(source_path=all_user_path, test_path=test_file_path, train_path=train_file_path)

    # put_files_to_hdfs(train_file_path, hdfs_input_file_path)

    # create_users_items_file(train_file_path)

    # # Clustering
    # # num = run_clustering_proposal_2_chi2(hdfs_input_file_path, NUMBER_OF_CLUSTERS)
    # num = run_clustering_proposal_2_chi2_ext1(hdfs_input_file_path, NUMBER_OF_CLUSTERS, MULTIPLIER)

    # Split input file
    # split_files_by_label(input_file_path, num=num)

    # open('./input/avg-file.txt', 'w')
    # with open(avg_file_path, 'r') as file, open('./input/avg-file.txt', 'a') as file2:
    #     for line in file:
    #         value, flag = line.strip().split('|')
    #         if(flag == 'a'):
    #             file2.write(value + '\n')

    # source_file_path = "./input/u.data"
    train_file_path = "./input/train_input.txt"
    test_file_path = "./input/test_input.txt"
    sim_path = "./hadoop_output/mfps.txt"
    avg_file_path = "./input/avg-file.txt"
    # # ratio = 0.8

    # split file
    # split_test_train(source_file_path, train_file_path, test_file_path, ratio)
    RMSE, F1 = evaluate(sim_path, train_file_path, test_file_path, avg_file_path, 10, 4)
    print(RMSE, F1)

    # MFPS
    # mfps_result = []

    # input_path = f"{HADOOP_PATH}/input/train_input.txt"
    # avg_ratings_path = f"{HADOOP_PATH}/input/avg-file.txt"
    # output_path = f"{HADOOP_PATH}/mfps-output/mfps"

    # result_data = run_mfps(
    #     input_path=input_path,
    #     avg_ratings_path=avg_ratings_path,
    #     output_path=output_path,
    # )
    # mfps_result.append(result_data)
    # mfps_result = [line for row in mfps_result for line in row]

    # output_path = f"./hadoop_output/mfps.txt"
    # write_data_to_file(output_path, mfps_result)
