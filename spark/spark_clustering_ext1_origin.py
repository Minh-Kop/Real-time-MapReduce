import time

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    split,
    col,
    when,
    collect_list,
    pandas_udf,
    sum as spark_sum,
    dense_rank,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    ArrayType,
)
import numpy as np


def run_spark_clustering(
    input_file_path,
    item_file_path,
    label_output_path,
    centroid_output_path,
    number_of_clusters=3,
    multiplier=1,
):
    ### Create a spark instance
    spark = (
        SparkSession.builder.appName("Clustering proposal 2 extension 1")
        # .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.memory", "12g")
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.memory.storageFraction", 0.1)
        .config("spark.driver.memoryOverheadFactor", 0.3)
        # .config("spark.sql.files.maxPartitionBytes", "230kb")
        # .config("spark.master", "local[1]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    @pandas_udf(DoubleType())
    def calculate_euclidean_distances(s1: pd.Series, s2: pd.Series) -> pd.Series:
        df1 = pd.DataFrame(s1.tolist())
        df2 = pd.DataFrame(s2.tolist())
        return np.sqrt(((df1 - df2) ** 2).sum(axis=1))

    ### Read input
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
    input_df.persist()

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
    items_df.persist()
    number_of_items = items_df.count()

    ### Clustering
    ## Chi2
    # Calculate user average
    user_avg_df = input_df.groupBy("user").mean("rating")

    # Create an user-item matrix
    matrix_df = user_avg_df.crossJoin(items_df)

    # Join observed into full matrix
    matrix_df = matrix_df.join(input_df, on=["user", "item"], how="left")

    # Add average for all null value
    filled_matrix_df = matrix_df.withColumn(
        "rating",
        when(col("rating").isNull(), col("avg(rating)")).otherwise(col("rating")),
    ).drop("avg(rating)")
    filled_matrix_df.persist()

    # Create user-ratings matrix
    user_ratings_df = filled_matrix_df.groupBy("user").agg(
        collect_list("rating").alias("ratings")
    )
    print("Create user-ratings matrix")
    user_ratings_df.persist()

    # Calculate observed value
    observed_df = filled_matrix_df.groupBy(["user", "category"]).agg(
        spark_sum("rating").alias("observed-value")
    )
    print("Calculate observed value")

    # Calculate category probabilities
    category_df = items_df.groupBy("category").count()
    category_df = category_df.withColumn("percentage", col("count") / number_of_items)
    print("Calculate category probabilities")

    # Sum user rating
    sum_rating_df = filled_matrix_df.groupBy("user").sum("rating")
    print("Sum user rating")

    # Calculate expected value
    expected_df = sum_rating_df.crossJoin(category_df)
    expected_df = expected_df.withColumn(
        "expected-value", col("percentage") * col("sum(rating)")
    ).drop("sum(rating)", "count", "percentage")
    print("Calculate expected value")

    # Calculate chi2
    chi2_df = expected_df.join(observed_df, on=["user", "category"])
    chi2_df = chi2_df.withColumn(
        "chi2",
        ((col("expected-value") - col("observed-value")) ** 2) / col("expected-value"),
    ).drop("expected-value", "observed-value")
    chi2_df = chi2_df.groupBy("user").sum("chi2").orderBy(col("sum(chi2)").desc())
    print("Calculate chi2")

    ## Get initial centroids
    # Get k*a user with highest chi2
    top_chi2_user_ratings_df = (
        chi2_df.drop("sum(chi2)")
        .limit(number_of_clusters * multiplier)
        .join(user_ratings_df, on="user")
    )
    print("Get k*a user with highest chi2")
    top_chi2_user_ratings_df.persist().count()

    # Free storage
    items_df.unpersist()
    input_df.unpersist()
    filled_matrix_df.unpersist()

    # Choose first line as first initial centroid
    print("Choose first line as first initial centroid")
    centroid_ratings_df = top_chi2_user_ratings_df.limit(1)
    centroid_ratings_df.persist()

    ## Get all remaining clusters
    for i in range(number_of_clusters - 1):
        print(f"\nLoop centroid {i}")
        # Get the new centroid's user
        current_centroid_df = centroid_ratings_df.limit(1)
        print("Get the new centroid")

        # Remove current centroid out of matrix
        current_centroid_user = current_centroid_df.collect()[0].user
        new_top_chi2_user_ratings_df = top_chi2_user_ratings_df.filter(
            f"user != {current_centroid_user}"
        )
        print("Remove current centroid out of matrix")
        new_top_chi2_user_ratings_df.persist()

        # Cross joint to get all user pair with that user
        distances_df = new_top_chi2_user_ratings_df.crossJoin(
            current_centroid_df.select(
                col("user").alias("user_"), col("ratings").alias("ratings_")
            )
        )

        # Calculate distances
        distances_df = (
            distances_df.withColumn(
                "distance",
                calculate_euclidean_distances(col("ratings"), col("ratings_")),
            )
            .drop("ratings", "ratings_")
            .orderBy(col("distance").desc())
        )

        # Add highest to the initial centroids
        next_centroid_df = new_top_chi2_user_ratings_df.join(
            distances_df.select("user").limit(1),
            on="user",
            how="left_semi",
        )
        print("Add highest to the initial centroids")

        new_centroid_ratings_df = next_centroid_df.union(
            centroid_ratings_df
        ).distinct()  #### Problem here
        new_centroid_ratings_df.persist().count()

        centroid_ratings_df.unpersist()
        centroid_ratings_df = new_centroid_ratings_df

        top_chi2_user_ratings_df.unpersist()
        top_chi2_user_ratings_df = new_top_chi2_user_ratings_df

    top_chi2_user_ratings_df.unpersist()

    ## KMeans
    i = 1
    while True:
        print(f"\nLoop KMeans {i}")
        print("Centroids")
        print("Matrix")

        # Calculate distances between each user to all centroids
        distances_df = user_ratings_df.drop("centroid").crossJoin(
            centroid_ratings_df.select(
                col("user").alias("centroid"),
                col("ratings").alias("centroid-ratings"),
            )
        )

        window_spec = Window.partitionBy("user").orderBy("distance")
        distances_df = (
            distances_df.withColumn(
                "distance",
                calculate_euclidean_distances(col("ratings"), col("centroid-ratings")),
            )
            .drop("centroid-ratings")
            .withColumn("rank", dense_rank().over(window_spec))
        )
        print("Calculate distances")

        # Label new centroids for all users based on the above distances
        new_user_ratings_df = distances_df.select("user", "ratings", "centroid").filter(
            "rank == 1"
        )
        print("Users with new centroids")
        new_user_ratings_df.persist().count()

        user_ratings_df.unpersist()
        user_ratings_df = new_user_ratings_df

        # Update centroids after labelling
        @pandas_udf(ArrayType(DoubleType()))
        def calculate_average_coordinates(s1: pd.Series) -> list:
            df1 = pd.DataFrame(s1.tolist())
            return df1.mean()

        new_centroid_ratings_df = user_ratings_df.groupBy(
            col("centroid").alias("user")
        ).agg(calculate_average_coordinates("ratings").alias("ratings"))
        print("Update centroids after labelling")
        new_centroid_ratings_df.persist()

        # Check if centroids converged
        number_of_converged_centroids = new_centroid_ratings_df.join(
            centroid_ratings_df.select("ratings"), on="ratings"
        ).count()
        print(
            f"Number of clusters: {number_of_clusters}, number of converged centroids: {number_of_converged_centroids}"
        )
        centroid_ratings_df.unpersist()
        centroid_ratings_df = new_centroid_ratings_df

        if number_of_converged_centroids == number_of_clusters:
            print("\nConverged\n")
            break

        i = i + 1

    print("Final matrix")

    ## Save to file
    user_ratings_df.drop("ratings").write.mode("overwrite").save(label_output_path)
    centroid_ratings_df.orderBy("user").write.mode("overwrite").save(
        centroid_output_path
    )


if __name__ == "__main__":
    input_file_path = "spark/input/input_file.txt"
    item_file_path = "spark/input/items.txt"
    # input_file_path = "spark/input/input_file_copy.txt"
    # item_file_path = "spark/input/items_copy.txt"
    label_output_path = "spark/output/labels"
    centroid_output_path = "spark/output/centroids"

    # Start timer
    start_time = time.perf_counter()

    run_spark_clustering(
        input_file_path, item_file_path, label_output_path, centroid_output_path
    )

    # End timer
    end_time = time.perf_counter()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time}s")
