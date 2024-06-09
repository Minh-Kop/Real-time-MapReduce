import os

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    col,
    when,
    collect_list,
    pandas_udf,
    sum as spark_sum,
    max as spark_max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)
import numpy as np


def write_to_file(df, file_name):
    df.write.mode("overwrite").csv("file://" + os.path.abspath(file_name), header=True)


if __name__ == "__main__":
    # input_file_path = "input/input_file.txt"
    # item_file_path = "input/items.txt"
    input_file_path = "input/input_file_copy.txt"
    item_file_path = "input/items_copy.txt"

    # Create a spark instance
    spark = (
        SparkSession.builder.appName("Clustering proposal 2 extension 1")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    @pandas_udf(DoubleType())
    def calculate_euclidean_distances(s1: pd.Series, s2: pd.Series) -> pd.Series:
        df1 = pd.DataFrame(s1.tolist())
        df2 = pd.DataFrame(s2.tolist())
        return np.sqrt(((df1 - df2) ** 2).sum(axis=1))

    ### read input
    input_df = spark.read.csv(
        path=input_file_path,
        schema=StructType(
            [
                StructField("user-item", StringType(), False),
                StructField("rating-time", StringType(), False),
            ]
        ),
        sep="\t",
    )

    user_item_split_col = split(input_df["user-item"], ";")
    rating_time_split_col = split(input_df["rating-time"], ";")

    input_df = input_df.select(
        user_item_split_col.getItem(0).alias("user").cast(IntegerType()),
        user_item_split_col.getItem(1).alias("item").cast(IntegerType()),
        rating_time_split_col.getItem(0).alias("rating").cast(IntegerType()),
    )

    items_df = spark.read.csv(
        path=item_file_path,
        schema=StructType(
            [
                StructField("item", IntegerType(), False),
                StructField("category", StringType(), False),
            ]
        ),
        sep="\t",
    )

    # Filter to remove unrated items
    items_df = items_df.join(input_df, on="item", how="left_semi")

    ### Clustering
    number_of_clusters = 3
    multiplier = 10

    ## Chi2
    # Calculate user average
    user_avg_df = input_df.groupBy("user").mean("rating")

    # Create a list user-item matrix
    matrix_df = user_avg_df.crossJoin(items_df)
    matrix_df.show()

    # Join observed into full matrix
    matrix_df = matrix_df.join(input_df, on=["user", "item"], how="left")
    matrix_df.show()
    print(matrix_df.count())

    # Add average for all null value
    filled_matrix_df = matrix_df.withColumn(
        "rating",
        when(col("rating").isNull(), col("avg(rating)")).otherwise(col("rating")),
    ).drop("avg(rating)")
    filled_matrix_df.show()

    # Calculate observed value
    observed_df = filled_matrix_df.groupBy(["user", "category"]).agg(
        spark_sum("rating").alias("observed-value")
    )
    observed_df.show()

    # Calculate category probabilities
    category_df = items_df.groupBy("category").count()
    category_df.show()
    category_df = category_df.withColumn("percentage", col("count") / items_df.count())
    category_df.show()

    # Sum user rating
    sum_rating_df = filled_matrix_df.groupBy("user").sum("rating")
    sum_rating_df.show()

    # Calculate expected value
    expected_df = sum_rating_df.crossJoin(category_df)
    expected_df.show()
    expected_df = expected_df.withColumn(
        "expected-value", col("percentage") * col("sum(rating)")
    ).drop("sum(rating)", "count", "percentage")

    expected_df.show()

    # Calculate chi2
    chi2_df = expected_df.join(observed_df, on=["user", "category"])
    chi2_df.show()
    chi2_df = chi2_df.withColumn(
        "chi2",
        ((col("expected-value") - col("observed-value")) ** 2) / col("expected-value"),
    ).drop("expected-value", "observed-value")
    chi2_df = chi2_df.groupBy("user").sum("chi2")
    chi2_df.show()

    ## Get initial centroids
    # Get k*a user with highest chi2
    centroids_df = chi2_df.orderBy(col("sum(chi2)").desc()).limit(
        number_of_clusters * multiplier
    )
    centroids_df.show()

    centroids_df = (
        centroids_df.drop("sum(chi2)")
        .join(filled_matrix_df.drop("category"), on="user")
        .orderBy(["user", "item"])
    )
    centroids_df.show()

    ## Calculate distance between each centroid
    # Get all rating in a array
    user_ratings_df = (
        centroids_df.drop("item")
        .groupBy("user")
        .agg(collect_list("rating").alias("ratings"))
        # .orderBy("user")
    )
    user_ratings_df.show()

    # Choose first line as first initial user
    centroids_df = user_ratings_df.limit(1)
    print("Centroids:")
    centroids_df.show()

    ## Get all remaining clusters
    for i in range(number_of_clusters - 1):
        # Get the new centroid's user
        current_centroid_df = centroids_df.limit(1)
        current_centroid_df.show()
        current_centroid_df.printSchema()

        user_ratings_df.show()
        user_ratings_df.printSchema()

        # Remove current centroid out of matrix
        user_ratings_df = user_ratings_df.filter(
            f"user != {current_centroid_df.collect()[0].user}"
        )
        user_ratings_df.show()

        # Cross joint and filter to get all user pair with that user
        distances_df = user_ratings_df.crossJoin(
            current_centroid_df.select(
                col("user").alias("user_"), col("ratings").alias("ratings_")
            )
        )
        distances_df.show()
        distances_df.printSchema()

        # Calculate distance
        distances_df = (
            distances_df.withColumn(
                "distance",
                calculate_euclidean_distances(col("ratings"), col("ratings_")),
            )
            .drop("ratings", "ratings_")
            .orderBy(col("distance").desc())
        )
        distances_df.show()

        # max_distance = distances_df.select(spark_max("distance")).collect()[0][0]
        # print(f"Max distance: {max_distance}")

        # Add highest to the initial centroids
        new_centroid_df = user_ratings_df.join(
            distances_df.limit(1),
            on="user",
            how="left_semi",
            # distances_df.filter(f"distance == {max_distance}"),
            # on="user",
            # how="left_semi",
        )
        new_centroid_df.show()

        centroids_df = new_centroid_df.union(centroids_df)
        centroids_df.show()

        ## KMeans
    while True:
        centroids_df.show()
        user_ratings_df.show()
        break
