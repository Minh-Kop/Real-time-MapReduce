import math

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_list,
    pandas_udf,
    array,
    mean as spark_mean,
    split,
)
from pyspark.sql.types import (
    IntegerType,
    DoubleType,
    ArrayType,
    StructType,
    StructField,
    StringType,
)
from pyspark import SparkConf


def run_mfps(input_file_path, output_path, alpha=10**-6):
    ## Create a spark instance
    spark_config = (
        SparkConf()
        .setAppName("MFPS")
        .set("spark.driver.memory", "5g")
        .set("spark.executor.memory", "7g")
        .set("spark.sql.shuffle.partitions", 8)
        .set("spark.executor.cores", 4)
        # .set("spark.executor.memory", "8g")
        # .set("spark.hadoop.fs.defaultFS", "file:///")
    )
    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

    ## Read input
    # Read input file
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

    # Wrap item, rating, time into an array
    temp_df = (
        input_df.withColumn("item-rating-time", array("item", "rating", "time"))
        .groupBy("user")
        .agg(collect_list("item-rating-time").alias("items_ratings_times"))
    )

    # Calculate average ratings
    average_df = (
        input_df.drop("item", "time")
        .groupBy("user")
        .agg(spark_mean("rating").alias("avg_rating"))
    )
    temp_df = temp_df.join(average_df, on="user")

    temp_df = (
        temp_df.join(
            temp_df.withColumnRenamed("items_ratings_times", "items_ratings_times_")
            .withColumnRenamed("user", "user_")
            .withColumnRenamed("avg_rating", "avg_rating_"),
            how="cross",
        )
        .filter(col("user") != col("user_"))
        .orderBy(["user", "user_"])
    )

    ## Calculate sim MFPS
    # Define UDF
    @pandas_udf(ArrayType(ArrayType(IntegerType())))
    def create_combinations(s1: pd.Series, s2: pd.Series) -> pd.Series:
        def create_combination_list(arr1, arr2):
            df1 = pd.DataFrame(
                np.vstack(arr1), columns=["item", "rating", "time"], dtype="int"
            )
            df2 = pd.DataFrame(
                np.vstack(arr2), columns=["item", "rating", "time"], dtype="int"
            )
            merged_df = df1.merge(df2, on="item", suffixes=("_1", "_2"))
            return merged_df.drop(columns="item").values.tolist()

        return s1.combine(s2, create_combination_list)

    @pandas_udf(IntegerType())
    def calculate_rating_commodity(combinations: pd.Series) -> pd.Series:
        return combinations.transform(len)

    @pandas_udf(IntegerType())
    def calculate_rating_usefulness(
        s1: pd.Series, rating_commodities: pd.Series
    ) -> pd.Series:
        return s1.transform(len) - rating_commodities

    @pandas_udf(IntegerType())
    def calculate_rating_details(
        rating_commodities: pd.Series,
        combinations: pd.Series,
        avg_ratings_1: pd.Series,
        avg_ratings_2: pd.Series,
    ) -> pd.Series:
        def calc_rating(rating_commodity, combination_list, avg_rating_1, avg_rating_2):
            if rating_commodity == 0:
                return 0

            df = (
                pd.DataFrame(
                    np.vstack(combination_list),
                    columns=["rating_1", "time_1", "rating_2", "time_2"],
                )
                .drop(columns=["time_1", "time_2"])
                .assign(avg_rating_1=avg_rating_1, avg_rating_2=avg_rating_2)
            )
            filtered_df = df[
                (
                    (df["rating_1"] > df["avg_rating_1"])
                    & (df["rating_2"] > df["avg_rating_2"])
                )
                | (
                    (df["rating_1"] < df["avg_rating_1"])
                    & (df["rating_2"] < df["avg_rating_2"])
                )
            ]
            return filtered_df.index.size

        return pd.Series(
            [
                calc_rating(
                    rating_commodity, combination_list, avg_rating_1, avg_rating_2
                )
                for rating_commodity, combination_list, avg_rating_1, avg_rating_2 in zip(
                    rating_commodities, combinations, avg_ratings_1, avg_ratings_2
                )
            ]
        )

    @pandas_udf(DoubleType())
    def calculate_rating_time(
        rating_commodities: pd.Series, combinations: pd.Series
    ) -> pd.Series:
        def calc_rating(rating_commodity, combination_list):
            if rating_commodity == 0:
                return 0

            df = pd.DataFrame(
                np.vstack(combination_list),
                columns=["rating_1", "time_1", "rating_2", "time_2"],
            ).drop(columns=["rating_1", "rating_2"])

            return (math.e ** (-alpha * (df["time_1"] - df["time_2"]).abs())).sum()

        return rating_commodities.combine(combinations, calc_rating)

    @pandas_udf(DoubleType())
    def calculate_mfps(
        rCom: pd.Series, rUse: pd.Series, rDet: pd.Series, rTim: pd.Series
    ) -> pd.Series:
        def calc_mfps(rc, ru, rd, rt):
            if rc == 0:
                return 0

            s = 1 + (
                1 / rc
                + (1 / ru if ru != 0 else 1.1)
                + (1 / rd if rd != 0 else 1.1)
                + (1 / rt if rt != 0 else 1.1)
            )
            return 1 / s

        return pd.Series(
            [
                calc_mfps(rc, ru, rd, rt)
                for rc, ru, rd, rt in zip(rCom, rUse, rDet, rTim)
            ]
        )

    # Rating commodity
    temp_df = temp_df.withColumn(
        "combinations",
        create_combinations(col("items_ratings_times"), col("items_ratings_times_")),
    ).withColumn("rc", calculate_rating_commodity(col("combinations")))
    print("Calculated rating commodity")

    # Rating usefulness
    temp_df = temp_df.withColumn(
        "ru", calculate_rating_usefulness(col("items_ratings_times_"), col("rc"))
    )
    print("Calculated rating usefulness")

    # Rating details
    temp_df = temp_df.withColumn(
        "rd",
        calculate_rating_details(
            col("rc"), col("combinations"), col("avg_rating"), col("avg_rating_")
        ),
    )
    print("Calculated rating details")

    # Rating time
    temp_df = temp_df.withColumn(
        "rt", calculate_rating_time(col("rc"), col("combinations"))
    ).drop("items_ratings_times", "items_ratings_times_", "avg_rating", "avg_rating_")
    print("Calculated rating time")

    # MFPS
    temp_df = temp_df.withColumn(
        "mfps", calculate_mfps(col("rc"), col("ru"), col("rd"), col("rt"))
    ).select("user", "user_", "mfps")
    print("Calculated MFPS")

    ## Save to file
    temp_df.write.mode("append").save(output_path)


if __name__ == "__main__":
    input_file_path = "spark/input/input_file.txt"
    # input_file_path = "spark/input/input_file_t.txt"
    output_path = "spark/output/mfps"

    import time

    # Start timer
    start_time = time.perf_counter()

    run_mfps(input_file_path, output_path)

    # End timer
    end_time = time.perf_counter()

    # Calculate elapsed time
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time}s")
