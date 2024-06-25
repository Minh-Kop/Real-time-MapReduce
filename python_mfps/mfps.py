import pandas as pd
import numpy as np
import math
import time
from sklearn.feature_selection import chi2, SelectKBest
from scipy.spatial.distance import cdist

start = time.time()

# Variable
alpha = 10**-6  # to calculate rating time
nCluster = 3  # to calculate top user for clustering


def kmeans_assign_labels(data_arr, centers):
    D = cdist(data_arr, centers)
    return np.argmin(D, axis=1)


def kmeans_update_centers(data_arr, labels, n_cluster):
    centers = np.zeros((n_cluster, data_arr.shape[1]))
    for i in range(n_cluster):
        data_arri = data_arr[labels == i, :]
        centers[i, :] = np.mean(data_arri, axis=0)
    return centers


def has_converged(centers, new_centers):
    return set([tuple(a) for a in centers]) == set([tuple(a) for a in new_centers])


def kmeans(data_arr, n_cluster, initial_centers):
    centers, labels, times = initial_centers, [], 0
    while True:
        labels = kmeans_assign_labels(data_arr, centers)
        new_centers = kmeans_update_centers(data_arr, labels, n_cluster)
        if has_converged(centers, new_centers):
            break
        centers = new_centers
        times += 1
    return (centers, labels, times)


# function for calculating mfps and its component
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


# Prepare data
data_df = pd.read_csv("./input/input_file.txt", sep="\t", names=["key", "value"])
data_df[["user", "item"]] = data_df["key"].str.split(";", expand=True)
data_df[["rating", "time"]] = data_df["value"].str.split(";", expand=True)

data_df = data_df.astype({"item": "int64", "rating": "float64"})
data_df.drop(["key", "value"], axis=1, inplace=True)
data_df.sort_values(by=["user", "item"], inplace=True)

item_df = pd.read_csv("./input/items.txt", sep="\t", names=["item", "categories"])
item_class = item_df["categories"].tolist()

avg_df = data_df.groupby("user")["rating"].mean()
ui_matrix_df = data_df.pivot(index="item", columns="user", values="rating")

# calculate top nCluster user for clustering
for user in ui_matrix_df.columns:
    ui_matrix_df[user] = ui_matrix_df[user].fillna(avg_df[user])

# ui_matrix_df.to_csv("./python_mfps/ui_matrix_df.csv")

selector = SelectKBest(chi2, k=nCluster)
users = selector.fit_transform(ui_matrix_df, item_class)

# Get the indices of selected features
selected_indices = selector.get_support(indices=True)

# Get the corresponding top user names
selected_users = ui_matrix_df.columns[selected_indices].tolist()


# data_df["item_rating_time"] = data_df.apply(
#     lambda row: [row["item"], row["rating"], row["time"]], axis=1
# )
# data_df.drop(["item", "rating", "time"], axis=1, inplace=True)
# data_df = data_df.groupby("user")["item_rating_time"].apply(list).reset_index()

# join_data_df = data_df.merge(data_df, how="cross", suffixes=("", "_"))
# join_data_df = join_data_df[join_data_df["user"] != join_data_df["user_"]]


# join_data_df["combination"] = join_data_df.apply(
#     lambda row: create_combinations(row["item_rating_time"], row["item_rating_time_"]),
#     axis=1,
# )

# join_data_df["rc"] = join_data_df.apply(
#     lambda row: rating_commodity(row["combination"]), axis=1
# )

# join_data_df["ru"] = join_data_df.apply(
#     lambda row: rating_usefulness(row["item_rating_time_"], row["rc"]), axis=1
# )

# join_data_df = join_data_df.merge(avg_df, on="user")
# join_data_df = join_data_df.merge(
#     avg_df, left_on="user_", right_on="user", suffixes=("", "_")
# )

# join_data_df["rd"] = join_data_df.apply(
#     lambda row: rating_detail(row["combination"], row["rating"], row["rating_"]),
#     axis=1,
# )

# join_data_df.drop(["rating", "rating_"], axis=1, inplace=True)

# join_data_df["rt"] = join_data_df.apply(
#     lambda row: rating_time(row["combination"]), axis=1
# )

# join_data_df.drop(
#     ["item_rating_time", "item_rating_time_", "combination"], axis=1, inplace=True
# )

# join_data_df["mfps"] = join_data_df.apply(
#     lambda row: mfps(row["rc"], row["ru"], row["rd"], row["rt"]), axis=1
# )
# end = time.time()
# join_data_df.to_csv("./python_mfps/ans.csv")

# print(join_data_df)

# print(f"Runtime: {end - start}")
