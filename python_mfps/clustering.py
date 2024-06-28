import pandas as pd
import numpy as np
import math
import time
from sklearn.feature_selection import chi2, SelectKBest
from scipy.spatial.distance import cdist

nCluster = 3  # to calculate top user for clustering
multiplier = 10
M = nCluster * multiplier

# Prepare data
data_df = pd.read_csv("./input/input_file.txt", sep="\t", names=["key", "value"])
data_df[["user", "item"]] = data_df["key"].str.split(";", expand=True)
data_df[["rating", "time"]] = data_df["value"].str.split(";", expand=True)

data_df = data_df.astype({"item": "int64", "rating": "float64"})
data_df.drop(["key", "value"], axis=1, inplace=True)
data_df.sort_values(by=["user", "item"], inplace=True)

# get classify list
item_df = pd.read_csv("./input/items.txt", sep="\t", names=["item", "categories"])
item_class = item_df["categories"].tolist()

# calculate user's average rating
avg_df = data_df.groupby("user")["rating"].mean()
ui_matrix_df = data_df.pivot(index="item", columns="user", values="rating")

# fill avg in matrix
for user in ui_matrix_df.columns:
    ui_matrix_df[user] = ui_matrix_df[user].fillna(avg_df[user])


# use SelectKBest to get top M user
selector = SelectKBest(chi2, k=M)
users = selector.fit_transform(ui_matrix_df, item_class)

# Get the indices of selected features
selected_indices = selector.get_support(indices=True)
# print(f"List of selected users: {ui_matrix_df.columns[selected_indices].tolist()}")

# Get user with highest chi2 value
chi2_scores = selector.scores_
index_of_highest_chi2 = selected_indices[np.argmax(chi2_scores[selected_indices])]
highest_chi2_user = ui_matrix_df.columns[index_of_highest_chi2]
# print(f"User with highest chi2 score among selected features: {highest_chi2_user}")

# Get the corresponding top centroid names and values
highest_users_df = pd.DataFrame(ui_matrix_df.columns[selected_indices])
ui_matrix_melt_df = ui_matrix_df.T.reset_index().melt(
    id_vars="user", var_name="item", value_name="rating"
)
highest_users_df = highest_users_df.merge(ui_matrix_melt_df, on="user")

# Create an array value
centroids_df = highest_users_df.groupby("user")["rating"].agg(list).reset_index()

# Get highest centroid and remove from list
initial_centroids_df = centroids_df[centroids_df["user"] == highest_chi2_user]
centroids_df = centroids_df[centroids_df["user"] != highest_chi2_user].reset_index(
    drop=True
)


def calculate_euclidean_distances(arr1, arr2):
    v1 = np.array(arr1.values[0])
    v2 = np.array(arr2)

    return np.linalg.norm(v1 - v2)


for i in range(nCluster - 1):
    # Calculate the distance of first element of initial centroids with all the leftover centroids
    centroids_df["distance"] = centroids_df.apply(
        lambda row: calculate_euclidean_distances(
            initial_centroids_df["rating"], row["rating"]
        ),
        axis=1,
    )

    # Add centroid with highest distance
    initial_centroids_df = pd.concat(
        [
            centroids_df[centroids_df["distance"] == centroids_df["distance"].max()]
            .drop("distance", axis=1)
            .reset_index(drop=True),
            initial_centroids_df,
        ],
        axis=0,
    ).reset_index(drop=True)

    centroids_df = centroids_df[
        centroids_df["user"] != initial_centroids_df["user"].values[0]
    ].reset_index(drop=True)


print(initial_centroids_df)
print(centroids_df)
