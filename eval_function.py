import os
import pandas as pd
import math

pd.options.mode.chained_assignment = None  # default='warn'


def split_test_train(source_path, train_path, test_path, ratio=0.8):
    if not os.path.exists(source_path):
        return

    if os.path.exists(test_path) or os.path.exists(train_path):
        with open(train_path, "w"), open(test_path, "w"):
            pass

    with open(source_path, "r") as src_file, open(test_path, "a") as test_file, open(
        train_path, "a"
    ) as train_file:
        lines = src_file.readlines()
        ratio_index = int(len(lines) * ratio)
        train_file.writelines(lines[:ratio_index])
        test_file.writelines(lines[ratio_index:])


def evaluate(k, number_of_recommended_items, sim_path, train_path, test_path, avg_path):
    # Read data from files
    train_df = pd.read_csv(
        train_path, delimiter="\t", names=["user_item", "rating_time"]
    )
    test_df = pd.read_csv(test_path, delimiter="\t", names=["user_item", "rating_time"])
    sim_df = pd.read_csv(sim_path, delimiter="\t", names=["user_user", "sim"])
    avg_df = pd.read_csv(
        avg_path, delimiter="\t", names=["user", "avg"], dtype={"user": str}
    )

    # Split user_item and rating_time columns into separate columns
    train_df[["user", "item"]] = train_df["user_item"].str.split(";", expand=True)
    train_df["rating"] = train_df["rating_time"].str.split(";", expand=True)[0]

    test_df[["user", "item"]] = test_df["user_item"].str.split(";", expand=True)
    test_df["rating"] = test_df["rating_time"].str.split(";", expand=True)[0]

    sim_df[["curr_user", "user"]] = sim_df["user_user"].str.split(";", expand=True)

    # Drop unnecessary columns
    train_df = train_df.drop(["user_item", "rating_time"], axis=1)
    test_df = test_df.drop(["user_item", "rating_time"], axis=1)
    sim_df = sim_df.drop("user_user", axis=1)

    # Merge average ratings into train_df and user_item_list
    train_df = pd.merge(train_df, avg_df, on="user")

    # Create user-item list with each record stores user and its item list
    user_item_list = test_df.groupby("user")["item"].agg(list).reset_index()
    user_item_list = pd.merge(user_item_list, avg_df, on="user")

    # Create a dictionary to store similarities

    sorted_df = (
        sim_df.groupby("curr_user")
        .apply(
            lambda x: x.sort_values(by="sim", ascending=False)[:k], include_groups=False
        )
        .reset_index(level=1, drop=True)
        .set_index(keys="user", append=True)
    )

    # Create a dataFrame to save predicted, observed value
    val_df = pd.DataFrame(columns=["user", "item", "pre_rating"])

    # Iterate over user_item_list
    for _, row in user_item_list.iterrows():
        u_user = row["user"]
        items = row["item"]
        avg = row["avg"]

        user_ratings = train_df[train_df["item"].isin(items)]
        user_ratings = user_ratings[
            user_ratings["user"].isin(sorted_df.loc[u_user].index)
        ]
        # print(user_ratings['user'].drop_duplicates())

        # Calculate similarities between the current user and all other users who have rated the items
        user_ratings["similarity"] = user_ratings["user"].map(
            lambda v_user: sorted_df.loc[(u_user, v_user), "sim"]
        )

        # Calculate the weighted sum of ratings and similarities
        user_ratings["weighted_sum"] = (
            user_ratings["rating"].astype(float) - user_ratings["avg"].astype(float)
        ) * user_ratings["similarity"].astype(float)

        # Sum of similarities for normalization
        sum_df = (
            user_ratings.groupby("item")
            .agg({"similarity": "sum", "weighted_sum": "sum"})
            .reset_index()
        )
        sum_df["pre_rating"] = avg + (sum_df["weighted_sum"] / sum_df["similarity"])
        sum_df["user"] = u_user

        val_df = pd.concat(
            [val_df, sum_df.drop(["similarity", "weighted_sum"], axis=1)]
        )
    val_df = pd.merge(val_df, test_df, on=["user", "item"], how="left")
    val_df.to_csv("./output/pre_obs_val", sep=",", index=False)

    dif = (val_df["rating"].astype(float) - val_df["pre_rating"].astype(float)) ** 2
    rmse = math.sqrt((dif / len(val_df)).sum())

    val_df = pd.merge(val_df, avg_df, on="user").astype(float)

    tp_df = (
        val_df[val_df["pre_rating"] > val_df["avg"]]
        .astype(float)
        .drop("rating", axis=1)
    )
    tp_df = pd.merge(tp_df, test_df.astype(float), on=["user", "item"])

    tp = len(tp_df[tp_df["rating"] > tp_df["avg"]])
    test_df = pd.merge(test_df, avg_df, on="user")

    precision = tp / len(val_df[val_df["pre_rating"] > val_df["avg"]])
    recall = tp / len(test_df[test_df["rating"].astype(float) > test_df["avg"]])

    f1 = 2 * (precision * recall) / (precision + recall)

    return rmse, f1


if __name__ == "__main__":
    source_file_path = "./input/u.data"
    item_file_path = "./input/u.item"
    all_user_path = "./input/input_file_copy.txt"
    test_file_path = "./input/test_input.txt"
    train_file_path = "./input/train_input.txt"

    train_file_path = "./input/train_input.txt"
    test_file_path = "./input/test_input.txt"
    sim_path = "./hadoop_output/mfps.txt"
    avg_file_path = "./input/avg-file.txt"

    RMSE, F1 = evaluate(10, 4, sim_path, train_file_path, test_file_path, avg_file_path)
    print(RMSE, F1)
