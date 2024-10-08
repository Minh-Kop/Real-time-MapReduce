import pandas as pd
import numpy as np
import time

from python_mfps.mfps import run_mfps
from python_mfps.clustering import run_clustering

if __name__ == "__main__":
    nCluster = 3  # to calculate top user for clustering
    multiplier = 10
    start_clustering = time.time()
    input_path = "input_file.txt"
    item_path = "items.txt"

    # Prepare data
    data_df = pd.read_csv(f"./input/{input_path}", sep="\t", names=["key", "value"])
    data_df[["user", "item"]] = data_df["key"].str.split(";", expand=True)
    data_df[["rating", "time"]] = data_df["value"].str.split(";", expand=True)

    data_df = data_df.astype({"item": "int64", "rating": "float64"})
    data_df.drop(["key", "value"], axis=1, inplace=True)
    data_df.sort_values(by=["user", "item"], inplace=True)
    item_df = pd.read_csv(
        f"./input/{item_path}", sep="\t", names=["item", "categories"]
    )

    item_df = data_df["item"].to_frame().merge(item_df, on="item")
    item_df = item_df.drop_duplicates(subset=["item"]).reset_index()

    # # test clustering
    # # class probability
    # item_class_df = item_df.groupby("categories").count().reset_index()
    # item_class_df["prob"] = item_class_df["item"] / len(item_class_df)
    # item_class_df[["categories", "prob"]].to_csv(
    #     "./python_mfps/output/average.csv", index=False
    # )

    # # user's average rating
    # avg_df = data_df.groupby("user")["rating"].mean().reset_index()
    # avg_df.to_csv("./python_mfps/output/average.csv", index=False)

    # # user's sum rating
    # user_item_rated = data_df.groupby("user")["item"].count().reset_index()
    # sum_df = data_df.groupby("user")["rating"].sum().reset_index()
    # users_df = sum_df.merge(avg_df, on="user", suffixes=("_s", "_a")).merge(
    #     user_item_rated, on="user"
    # )
    # users_df["sum"] = users_df["rating_s"] + users_df["rating_a"] * (
    #     len(item_class_df) - users_df["item"]
    # )
    # users_df.drop(["rating_s", "item"], axis=1, inplace=True)
    # users_df = users_df.rename(columns={"rating_a": "avg"})

    # # user's E
    # users_df = users_df.merge(item_class_df, how="cross")
    # users_df["E"] = users_df["sum"] * users_df["prob"]
    # E_df = users_df.drop(["avg", "sum", "item", "prob"], axis=1)
    # E_df.sort_values(by=["user", "categories"], inplace=True, ascending=False)
    # E_df[["user", "categories", "E"]].to_csv("./python_mfps/output/E.csv", index=False)

    # # user's O
    # user = data_df["user"].unique()

    # O_df = item_df.merge(pd.DataFrame(user, columns=["user"]), how="cross")
    # O_df = O_df.rename(columns={"0": "user"})

    # O_df = O_df.merge(data_df, on=["user", "item"], how="left").merge(
    #     avg_df, on="user", how="left", suffixes=("", "_a")
    # )
    # O_df["rating"] = O_df["rating"].fillna(O_df["rating_a"])
    # O_df.drop(["time", "rating_a"], axis=1, inplace=True)

    # O_df = O_df.groupby(["user", "categories"])["rating"].sum().reset_index()
    # O_df.sort_values(by=["user", "categories"], inplace=True, ascending=False)
    # O_df[["user", "categories", "rating"]].to_csv(
    #     "./python_mfps/output/O.csv", index=False
    # )

    # Chi2_df = O_df.merge(E_df, on=["user", "categories"])
    # Chi2_df["chi2"] = (Chi2_df["E"] - Chi2_df["rating"]) ** 2 / Chi2_df["E"]
    # Chi2_df[["user", "categories", "chi2"]].to_csv(
    #     "./python_mfps/output/temp.csv", index=False
    # )
    # Chi2_df = Chi2_df.groupby("user")["chi2"].sum().reset_index()
    # Chi2_df.sort_values(by=["chi2"], inplace=True, ascending=False)
    # Chi2_df[["user", "chi2"]].to_csv("./python_mfps/output/Chi2.csv", index=False)

    # print(Chi2_df)

    # run clustering
    user_df = run_clustering(data_df, item_df, nCluster, multiplier)
    user_df.to_csv("./python_mfps/output/labeling.csv", index=False)
    centroid_list = user_df["centroid"].unique().tolist()
    end_clustering = time.time()
    print(f"CLustering runtime:{end_clustering - start_clustering}")
    start_mfps = time.time()

    a = pd.DataFrame()
    
    # run mfps
    for i in range(nCluster):
        # print(f"MFPS loop: {i + 1}")
        mfps_data_df = user_df[user_df["centroid"] == centroid_list[i]]

        with open(f"./python_mfps/output/cluster{i + 1}", "w") as file:
            for user in mfps_data_df["user"].tolist():
                file.write(f"{user}\n")

        mfps_data_df = mfps_data_df.merge(data_df, on="user")
        mfps_data_df.drop("centroid", axis=1, inplace=True)

        mfps_result = run_mfps(mfps_data_df)

        mfps_result["user_user"] = (
            mfps_result["user"].astype(str) + ";" + mfps_result["user_"].astype(str)
        )
        
        mfps_result.drop(
            ["ru", "rc", "rd", "rt", "user", "user_"], axis=1, inplace=True
        )
        a = pd.concat([a, mfps_result])
        mfps_result.to_csv(
            f"./python_mfps/output/sim_{i + 1}.csv", index=False, header=False, sep="\t"
        )


    end_mfps = time.time()
    print(f"MFPS runtime: {end_mfps - start_mfps}")
    a[['user_user','mfps']].to_csv(f"./python_mfps/output/sim.txt", index=False, header=False, sep="\t")
    print(
        f"Total run time: {(end_clustering - start_clustering) + (end_mfps - start_mfps)}"
    )
