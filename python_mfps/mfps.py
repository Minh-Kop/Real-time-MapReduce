import pandas as pd
import numpy as np
import math

alpha = 10**-6

data_df = pd.read_csv("./input/input_file_copy.txt", sep="\t", names=["key", "value"])
data_df[["user", "item"]] = data_df["key"].str.split(";", expand=True)
data_df[["rating", "time"]] = data_df["value"].str.split(";", expand=True)

data_df = data_df.astype({"item": "int64", "rating": "float64"})

data_df.drop(["key", "value"], axis=1, inplace=True)
data_df.sort_values(by=["user", "item"], inplace=True)

avg_df = data_df.groupby("user")["rating"].mean().reset_index()

data_df["item_rating_time"] = data_df.apply(
    lambda row: [row["item"], row["rating"], row["time"]], axis=1
)
data_df.drop(["item", "rating", "time"], axis=1, inplace=True)
data_df = data_df.groupby("user")["item_rating_time"].apply(list).reset_index()

join_data_df = data_df.merge(data_df, how="cross", suffixes=("", "_"))
join_data_df = join_data_df[join_data_df["user"] != join_data_df["user_"]]


def create_combinations(arr1, arr2):
    df1 = pd.DataFrame(
        np.vstack(arr1), columns=["item", "rating", "time"], dtype="float"
    )
    df2 = pd.DataFrame(
        np.vstack(arr2), columns=["item", "rating", "time"], dtype="float"
    )

    merged_df = df1.merge(df2, on="item", suffixes=("_1", "_2"))
    return merged_df.drop(columns="item").values.tolist()


def rating_commodity(arr):
    return len(arr)


def rating_usefulness(arr, rc):
    return len(arr) - rc


def rating_usefulness(arr, rc):
    return len(arr) - rc


def rating_detail(arr, avg1, avg2):
    if len(arr) == 0:
        return 0

    df = (
        pd.DataFrame(
            np.vstack(arr),
            columns=["rating_1", "time_1", "rating_2", "time_2"],
        )
        .drop(columns=["time_1", "time_2"])
        .assign(avg_rating_1=avg1, avg_rating_2=avg2)
    )

    filtered_df = df[
        ((df["rating_1"] > df["avg_rating_1"]) & (df["rating_2"] > df["avg_rating_2"]))
        | (
            (df["rating_1"] < df["avg_rating_1"])
            & (df["rating_2"] < df["avg_rating_2"])
        )
    ]

    return len(filtered_df)


def rating_time(arr):
    if len(arr) == 0:
        return 0

    df = pd.DataFrame(
        np.vstack(arr),
        columns=["rating_1", "time_1", "rating_2", "time_2"],
    ).drop(columns=["rating_1", "rating_2"])

    return (math.e ** (-alpha * (df["time_1"] - df["time_2"]).abs())).sum()


def mfps(rc, ru, rd, rt):
    if rc == 0:
        return 0

    s = 1 + (
        1 / rc
        + (1 / ru if ru != 0 else 1.1)
        + (1 / rd if rd != 0 else 1.1)
        + (1 / rt if rt != 0 else 1.1)
    )
    return 1 / s


join_data_df["combination"] = join_data_df.apply(
    lambda row: create_combinations(row["item_rating_time"], row["item_rating_time_"]),
    axis=1,
)

join_data_df["rc"] = join_data_df.apply(
    lambda row: rating_commodity(row["combination"]), axis=1
)

join_data_df["ru"] = join_data_df.apply(
    lambda row: rating_usefulness(row["item_rating_time_"], row["rc"]), axis=1
)

join_data_df = join_data_df.merge(avg_df, on="user")
join_data_df = join_data_df.merge(
    avg_df, left_on="user_", right_on="user", suffixes=("", "_")
)

join_data_df["rd"] = join_data_df.apply(
    lambda row: rating_detail(row["combination"], row["rating"], row["rating_"]),
    axis=1,
)

join_data_df.drop(["rating", "rating_"], axis=1, inplace=True)

join_data_df["rt"] = join_data_df.apply(
    lambda row: rating_time(row["combination"]), axis=1
)

join_data_df.drop(
    ["item_rating_time", "item_rating_time_", "combination"], axis=1, inplace=True
)

join_data_df["mfps"] = join_data_df.apply(
    lambda row: mfps(row["rc"], row["ru"], row["rd"], row["rt"]), axis=1
)

print(join_data_df)
