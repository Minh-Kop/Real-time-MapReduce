import re

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import split
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)


def get_filename(file_path):
    pattern = "(.+)\.[^\.]*$"
    match = re.search(pattern, file_path)
    if match:
        name = match.group(1)
        return name
    return file_path


def split_files_by_label(input_file_path, label_file_path, centroid_file_path):
    ## Create a spark instance
    spark = (
        SparkSession.builder.appName("Spilt files")
        # .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", 8)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    ## Read input
    # Store input file into a DataFrame
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
        rating_time_split_col.getItem(1).alias("time").cast(IntegerType()),
    )

    # Store labels file into a DataFrame
    labels_df = spark.read.load(label_file_path)

    ## Split files
    # Join input_df with labels_df to get centroid cluster
    input_df = input_df.join(labels_df, on="user")

    # Read centroid list
    centroids = spark.read.load(centroid_file_path).collect()
    centroid_rows = []

    # Iterate through each centroid to split input file
    i = 0
    input_filename = get_filename(input_file_path)
    for centroid in centroids:
        input_df.filter(f"centroid == {centroid[0]}").drop("centroid").write.mode(
            "overwrite"
        ).save(f"{input_filename}_{i}")

        centroid_rows.append(Row(user=i, ratings=centroid[1]))
        i = i + 1

    # Save new centroids file with user ranges from 0 to number of centroids - 1 to match split input files' name
    spark.createDataFrame(centroid_rows).write.mode("overwrite").save(
        centroid_file_path
    )
    return input_filename


if __name__ == "__main__":
    # input_file_path = "input/input_file.txt"
    input_file_path = "spark/input/input_file_copy.txt"
    label_file_path = "spark/output/labels"
    centroid_file_path = "spark/output/centroids"

    import time

    # Start timer
    start_time = time.perf_counter()

    split_files_by_label(input_file_path, label_file_path, centroid_file_path)

    # End timer
    end_time = time.perf_counter()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time}s")
