import pandas as pd
import numpy as np
import time

from python_mfps.mfps import run_mfps
from python_mfps.clustering import run_clustering

if __name__ == "__main__":
    nCluster = 3  # to calculate top user for clustering
    multiplier = 10
    start = time.time()

    # Prepare data
    data_df = pd.read_csv("./input/input_file.txt", sep="\t", names=["key", "value"])
    data_df[["user", "item"]] = data_df["key"].str.split(";", expand=True)
    data_df[["rating", "time"]] = data_df["value"].str.split(";", expand=True)

    data_df = data_df.astype({"item": "int64", "rating": "float64"})
    data_df.drop(["key", "value"], axis=1, inplace=True)
    data_df.sort_values(by=["user", "item"], inplace=True)
    item_df = pd.read_csv("./input/items.txt", sep="\t", names=["item", "categories"])

    # run clustering
    user_df = run_clustering(data_df, item_df, nCluster, multiplier)
    centroid_list = user_df["centroid"].unique().tolist()

    # # run mfps
    # for i in range(nCluster):
    #     print(f"MFPS loop: {i + 1}")
    #     mfps_data_df = user_df[user_df["centroid"] == centroid_list[i]]

    #     with open(f"./python_mfps/output/cluster{i + 1}", "w") as file:
    #         for user in mfps_data_df["user"].tolist():
    #             file.write(f"{user}\n")

    #     mfps_data_df = mfps_data_df.merge(data_df, on="user")
    #     mfps_data_df.drop("centroid", axis=1, inplace=True)

    #     mfps_result = run_mfps(mfps_data_df)
    #     mfps_result.to_csv(f"./python_mfps/output/ans{i + 1}.csv")

    end = time.time()
    print(f"Runtime: {end - start}")
