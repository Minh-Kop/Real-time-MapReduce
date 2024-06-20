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
)
from pyspark.sql.types import IntegerType, DoubleType, ArrayType
from pyspark import SparkConf


# input_file_path = "./input/input_file.txt"
# item_file_path = "./input/items.txt"
input_file_path = "input/input_file_copy.txt"
item_file_path = "input/items_copy.txt"

spark_config = (
    SparkConf()
    .setAppName("spark_clustering")
    .set("spark.driver.memory", "8g")
    .set("spark.driver.maxResultSize", "4g")
    .set("spark.executor.memory", "8g")
)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
alpha = 10**-6

### Read input
# Read input file
pd_input_df = pd.read_csv(input_file_path, sep="\t", names=["user-item", "rating-time"])
pd_input_df[["user", "item"]] = pd_input_df["user-item"].str.split(";", expand=True)
pd_input_df[["rating", "time"]] = pd_input_df["rating-time"].str.split(";", expand=True)
pd_input_df = pd_input_df.drop(["user-item", "rating-time"], axis=1)
pd_input_df["rating"] = pd_input_df["rating"].astype(float)

# Transfer from pandas to spark DataFrame
spark_input_df = (
    spark.createDataFrame(pd_input_df)
    .withColumn("item", col("item").cast("integer"))
    .withColumn("time", col("time").cast("integer"))
)

temp_df = (
    spark_input_df.withColumn("item-rating-time", array("item", "rating", "time"))
    .groupBy("user")
    .agg(collect_list("item-rating-time").alias("items_ratings_times"))
)

average_df = (
    spark_input_df.drop("item", "time")
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
            calc_rating(rating_commodity, combination_list, avg_rating_1, avg_rating_2)
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
        [calc_mfps(rc, ru, rd, rt) for rc, ru, rd, rt in zip(rCom, rUse, rDet, rTim)]
    )


## Rating commodity
temp_df = temp_df.withColumn(
    "combinations",
    create_combinations(col("items_ratings_times"), col("items_ratings_times_")),
).withColumn("rc", calculate_rating_commodity(col("combinations")))
print("Calculated rating commodity")

## Rating usefulness
temp_df = temp_df.withColumn(
    "ru", calculate_rating_usefulness(col("items_ratings_times_"), col("rc"))
)
print("Calculated rating usefulness")

## Rating details
temp_df = temp_df.withColumn(
    "rd",
    calculate_rating_details(
        col("rc"), col("combinations"), col("avg_rating"), col("avg_rating_")
    ),
)
print("Calculated rating details")

## Rating time
temp_df = temp_df.withColumn(
    "rt", calculate_rating_time(col("rc"), col("combinations"))
).drop("items_ratings_times", "items_ratings_times_", "avg_rating", "avg_rating_")
print("Calculated rating time")

## MFPS
temp_df = temp_df.withColumn(
    "mfps", calculate_mfps(col("rc"), col("ru"), col("rd"), col("rt"))
)
print("Calculated MFPS")

temp_df.select(["user", "user_", "rc", "ru", "rd", "rt", "mfps"]).show()
