import os
import pandas as pd
import numpy as np

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


def evaluate(k, number_of_recommend_items, sim_path, train_path, test_path, avg_path):
    # Create a training set DataFrame
    training_df = pd.read_csv(
        train_path, delimiter="\t", names=["user_item", "rating_time"]
    )
    training_df[["user", "item"]] = training_df["user_item"].str.split(";", expand=True)
    training_df["rating"] = training_df["rating_time"].str.split(";", expand=True)[0]
    training_df = training_df.drop(["user_item", "rating_time"], axis=1)
    training_df = training_df.astype({"user": "int", "item": "int", "rating": "int"})

    # Create a test set DataFrame
    test_df = pd.read_csv(test_path, delimiter="\t", names=["user_item", "rating_time"])
    test_df[["user", "item"]] = test_df["user_item"].str.split(";", expand=True)
    test_df["rating"] = test_df["rating_time"].str.split(";", expand=True)[0]
    test_df = test_df.drop(["user_item", "rating_time"], axis=1)
    test_df = test_df.astype(int)

    # Create a similarity set
    sim_df = pd.read_csv(sim_path, delimiter="\t", names=["user_user", "sim"])
    sim_df[["user_1", "user_2"]] = sim_df["user_user"].str.split(";", expand=True)
    sim_df = sim_df.drop("user_user", axis=1)
    sim_df = sim_df.astype({"user_1": "int", "user_2": "int"})
    # Sort similarity based on user in similarity set
    sorted_sim_df = (
        sim_df.groupby("user_1")
        .apply(lambda x: x.sort_values(by="sim", ascending=False), include_groups=False)
        .reset_index(level=1, drop=True)
        .set_index(keys="user_2", append=True)
    )

    # Create an average rating set
    avg_rating_df = pd.read_csv(avg_path, delimiter="\t", names=["user", "avg_rating"])

    # Merge average rating set into training set
    training_df = pd.merge(training_df, avg_rating_df, on="user")

    # Create a test users set with each record stores an user's item list
    test_users_df = test_df.groupby("user")["item"].agg(list).reset_index()
    test_users_df = pd.merge(test_users_df, avg_rating_df, on="user")

    # Create a dataFrame to save RMSE, F1 score
    result_df = pd.DataFrame(
        columns=[
            "user",
            "number_of_correct_predict_items",
            "number_of_truth_set_items",
            "RMSE",
            "number_of_items",
        ]
    )

    # Iterate over test users set
    for _, row in test_users_df.iterrows():
        user_1 = row["user"]
        items = row["item"]
        user_1_avg_rating = row["avg_rating"]
        sorted_users_df = sorted_sim_df.loc[user_1]

        # Check if the number of test set's items are enough to predict
        if len(items) < number_of_recommend_items:
            continue

        predict_df = pd.DataFrame(columns=["item", "rating", "predict_rating", "rmse"])

        for item in items:
            # Filter training set DataFrame with only rows that have this item
            filtered_training_df = training_df[training_df["item"] == item]

            # Get top k users who rated this item
            top_k_users_df = sorted_users_df[
                sorted_users_df.index.isin(filtered_training_df["user"])
            ][:k]

            # Check if the number of top neighbors are enough to use for predict
            if len(top_k_users_df) < k:
                continue

            # Continue filtering training set DataFrame with users in top k users
            filtered_training_df = filtered_training_df[
                filtered_training_df["user"].isin(top_k_users_df.index)
            ]

            # Add similarities between the current user and all other users in filtered training set
            filtered_training_df["sim"] = filtered_training_df["user"].map(
                lambda user_2: top_k_users_df.loc[user_2, "sim"]
            )

            # Calculate the weighted sum of ratings and similarities
            filtered_training_df["weighted_error"] = (
                filtered_training_df["rating"] - filtered_training_df["avg_rating"]
            ) * filtered_training_df["sim"]

            # Calculate predict rating
            predict_rating = user_1_avg_rating + (
                filtered_training_df["weighted_error"].sum()
                / filtered_training_df["sim"].sum()
            )
            rating = test_df.loc[
                (test_df["user"] == user_1) & (test_df["item"] == item), "rating"
            ].values[0]
            predict_df = pd.concat(
                [
                    predict_df,
                    pd.DataFrame(
                        [
                            [
                                item,
                                rating,
                                predict_rating,
                                pow(predict_rating - rating, 2),
                            ]
                        ],
                        columns=["item", "rating", "predict_rating", "rmse"],
                    ),
                ]
            ).reset_index(drop=True)

        # predict_df = predict_df[predict_df["predict_rating"] > user_1_avg_rating]
        # predict_df = predict_df.sort_values(by="predict_rating", ascending=False)[
        #     :number_of_recommend_items
        # ]
        recommend_item_df = predict_df[
            predict_df["predict_rating"] > user_1_avg_rating
        ].sort_values(by="predict_rating", ascending=False, ignore_index=True)[
            :number_of_recommend_items
        ][
            ["item"]
        ]

        if len(recommend_item_df) < number_of_recommend_items:
            continue

        # Create truth set with only items with ratings > avg rating
        test_user_df = test_df.loc[
            (test_df["user"] == user_1) & (test_df["rating"] > user_1_avg_rating)
        ]

        # Merge with recommend set to get the correct predict items
        merge_df = recommend_item_df.merge(
            test_user_df,
            on="item",
        )
        number_of_correct_predict_items = len(merge_df)
        number_of_truth_set_items = len(test_user_df.index)

        result_df = pd.concat(
            [
                result_df,
                pd.DataFrame(
                    [
                        [
                            user_1,
                            number_of_correct_predict_items,
                            number_of_truth_set_items,
                            predict_df["rmse"].sum(),
                            len(predict_df.index),
                        ]
                    ],
                    columns=[
                        "user",
                        "number_of_correct_predict_items",
                        "number_of_truth_set_items",
                        "RMSE",
                        "number_of_items",
                    ],
                ),
            ]
        ).reset_index(drop=True)

    # Calculate RMSE
    rmse = np.sqrt(result_df["RMSE"].sum() / result_df["number_of_items"].sum())

    # Calculate F1-score
    number_of_correct_predict_items = result_df["number_of_correct_predict_items"].sum()
    number_of_recommend_items_ = number_of_recommend_items * len(result_df.index)
    number_of_truth_set_items = result_df["number_of_truth_set_items"].sum()

    precision = number_of_correct_predict_items / number_of_recommend_items_
    recall = number_of_correct_predict_items / number_of_truth_set_items
    f1_score = 2 * precision * recall / (precision + recall)

    return rmse, f1_score


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
    print(f"RMSE: {RMSE}\nF1-score: {F1}")
