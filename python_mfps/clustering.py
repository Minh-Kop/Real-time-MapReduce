import pandas as pd
import numpy as np
import math
import time
from sklearn.feature_selection import chi2, SelectKBest
from scipy.spatial.distance import cdist


# function
def calculate_euclidean_distances(arr1, arr2):
    v1 = np.array(arr1)
    v2 = np.array(arr2)

    return np.linalg.norm(v1 - v2)


def centroid_labeling(arr, centroids):
    centroids["distance"] = centroids.apply(
        lambda row: calculate_euclidean_distances(arr, row["rating"]), axis=1
    )

    min_distance_index = centroids["distance"].idxmin()
    # Retrieve the row with the minimum value in the 'distance' column
    min_distance_row = centroids.loc[min_distance_index]

    return min_distance_row.centroid


def run_clustering(data_df, item_df, nCluster, multiplier):
    # get classify list
    item_class = item_df["categories"].tolist()

    # calculate user's average rating
    avg_df = data_df.groupby("user")["rating"].mean()
    ui_matrix_df = data_df.pivot(index="item", columns="user", values="rating")

    # fill avg in matrix
    for user in ui_matrix_df.columns:
        ui_matrix_df[user] = ui_matrix_df[user].fillna(avg_df[user])

    # use SelectKBest to get top M user
    selector = SelectKBest(chi2, k=nCluster * multiplier)
    users = selector.fit_transform(ui_matrix_df, item_class)

    # Get the indices of selected features
    selected_indices = selector.get_support(indices=True)
    # print(f"List of selected users: {ui_matrix_df.columns[selected_indices].tolist()}")

    # Get user with highest chi2 value
    chi2_scores = selector.scores_
    selected_features = ui_matrix_df.columns[selected_indices]

    feature_scores = pd.DataFrame(
        {"Feature": ui_matrix_df.columns, "Chi2 Score": chi2_scores}
    ).sort_values(by="Chi2 Score", ascending=False)

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

    # Calculate initial centroids
    for i in range(nCluster - 1):
        # Calculate the distance of first element of initial centroids with all the leftover centroids
        centroids_df["distance"] = centroids_df.apply(
            lambda row: calculate_euclidean_distances(
                initial_centroids_df["rating"].iloc[0], row["rating"]
            ),
            axis=1,
        )

        max_distance_index = centroids_df["distance"].idxmax()

        # Retrieve the row with the maximum value in the 'distance' column
        max_distance_row = centroids_df.loc[max_distance_index]

        # Add centroid with highest distance
        initial_centroids_df = pd.concat(
            [
                max_distance_row.to_frame()
                .T.drop("distance", axis=1)
                .reset_index(drop=True),
                initial_centroids_df,
            ],
            axis=0,
        ).reset_index(drop=True)

        centroids_df = centroids_df[
            centroids_df["user"] != initial_centroids_df["user"].values[0]
        ].reset_index(drop=True)

    # User array rating
    users_df = ui_matrix_melt_df.groupby("user")["rating"].agg(list).reset_index()
    current_centroids_df = initial_centroids_df.rename(
        columns={"user": "centroid"}
    ).copy()
    old_centroid_df = current_centroids_df.copy()

    i = 0
    # Kmeans
    while True:
        # print(f"Kmeans loop: {i}")
        i += 1
        users_df["centroid"] = users_df.apply(
            lambda row: centroid_labeling(row["rating"], current_centroids_df.copy()),
            axis=1,
        )

        current_centroids_df = (
            users_df.groupby("centroid")["rating"].agg(list).reset_index()
        )

        current_centroids_df["new_centroid_rating"] = current_centroids_df.apply(
            lambda row: np.mean(row["rating"], axis=0), axis=1
        )

        current_centroids_df = (
            current_centroids_df.drop("rating", axis=1)
            .merge(
                old_centroid_df,
                on="centroid",
            )
            .rename(columns={"new_centroid_rating": "rating", "rating": "old_rating"})
        )

        if current_centroids_df["rating"].equals(current_centroids_df["old_rating"]):
            break
        else:
            current_centroids_df.drop("old_rating", axis=1, inplace=True)
            old_centroid_df = current_centroids_df.copy()

    return users_df[["user", "centroid"]]
