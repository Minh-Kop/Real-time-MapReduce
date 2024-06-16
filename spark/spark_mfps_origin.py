import pyspark, os
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    col,
    when,
    collect_list,
    coalesce,
    concat,
    udf,
    lit,
    pandas_udf,
    concat_ws,
    array,
)
from pyspark.sql.types import (
    DoubleType,
    ArrayType,
    NullType,
    IntegerType,
    StructType,
    StructField,
    StringType,
    FloatType,
)
from pyspark import SparkConf
import math


def write_to_file(df, file_name):
    df.coalesce(1).write.csv(
        "file://" + os.path.abspath(f"./spark/{file_name}"), header=True
    )


input_file_path = "./input/input_file.txt"
item_file_path = "./input/items.txt"
spark_config = (
    SparkConf()
    .setAppName("spark_clustering")
    .set("spark.driver.memory", "8g")
    .set("spark.executor.cores", "4")
    .set("spark.driver.maxResultSize", "4g")
    .set("spark.executor.memory", "8g")
)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
alpha = 10**-6
### read input
# read input file
pd_input_df = pd.read_csv(input_file_path, sep="\t", names=["user-item", "rating-time"])
pd_input_df[["user", "item"]] = pd_input_df["user-item"].str.split(";", expand=True)
pd_input_df[["rating", "time"]] = pd_input_df["rating-time"].str.split(";", expand=True)
pd_input_df = pd_input_df.drop(["user-item", "rating-time"], axis=1)
pd_input_df["rating"] = pd_input_df["rating"].astype(float)

# read item file
pd_item_df = pd.read_csv(item_file_path, sep="\t", names=["item", "categories"]).astype(
    str
)
spark_item_df = spark.createDataFrame(pd_item_df).drop("categories")

# transfer from pandas to spark DataFrame
spark_input_df = (
    spark.createDataFrame(pd_input_df)
    .withColumn("item", col("item").cast("integer"))
    .withColumn("time", col("time").cast("double"))
    .orderBy(["user", "item"])
)

temp_df = (
    spark_input_df.withColumn("item-rating-time", array("item", "rating", "time"))
    .groupBy("user")
    .agg(collect_list("item-rating-time").alias("items_ratings_times"))
    .orderBy("user")
)

average_df = (
    spark_input_df.drop("item", "time").groupBy("user").mean("rating").orderBy("user")
)

temp_df = (
    temp_df.join(
        temp_df.withColumnRenamed(
            "items_ratings_times", "items_ratings_times_"
        ).withColumnRenamed("user", "user_")
    )
    .filter(col("user") != col("user_"))
    .orderBy(["user", "user_"])
)


@pandas_udf(IntegerType())
def rating_commodity_p_udf(arr1: pd.Series, arr2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2):
        s = 0
        index1 = 0
        index2 = 0
        len1 = len(ar1)
        len2 = len(ar2)
        while True:
            if len1 == index1 or len2 == index2:
                break

            if ar1[index1][0] != ar2[index2][0]:
                if ar1[index1][0] > ar2[index2][0]:
                    index2 += 1
                else:
                    index1 += 1
            else:
                s += 1
                index1 += 1
                index2 += 1
        return int(s)

    return arr1.combine(arr2, lambda x, y: calc_rating(x, y))


@pandas_udf(IntegerType())
def rating_usefulness_p_udf(arr1: pd.Series, arr2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2):
        s = 0
        index1 = 0
        index2 = 0
        len1 = len(ar1)
        len2 = len(ar2)
        while True:
            if len2 == index2:
                break

            if len1 != index1:
                if ar1[index1][0] != ar2[index2][0]:
                    if ar1[index1][0] > ar2[index2][0]:
                        s += 1
                        index2 += 1
                    else:
                        index1 += 1
                else:
                    index1 += 1
                    index2 += 1
            else:
                s += 1
                index2 += 1
        return int(s)

    return arr1.combine(arr2, lambda x, y: calc_rating(x, y))


@pandas_udf(IntegerType())
def rating_detail_p_udf(
    arr1: pd.Series, arr2: pd.Series, avg1: pd.Series, avg2: pd.Series
) -> pd.Series:
    def calc_rating(ar1, ar2, av1, av2):
        s = 0
        index1 = 0
        index2 = 0
        len1 = len(ar1)
        len2 = len(ar2)
        while True:
            if len1 == index1 or len2 == index2:
                break

            if ar1[index1][0] != ar2[index2][0]:
                if ar1[index1][0] > ar2[index2][0]:
                    index2 += 1
                else:
                    index1 += 1
            else:
                if ar1[index1][1] > av1 and ar2[index2][1] > av2:
                    s += 1
                if ar1[index1][1] < av1 and ar2[index2][1] < av2:
                    s += 1
                index1 += 1
                index2 += 1

        return int(s)

    return pd.Series(
        [calc_rating(x, y, av1, av2) for x, y, av1, av2 in zip(arr1, arr2, avg1, avg2)]
    )


@pandas_udf(FloatType())
def rating_time_p_udf(arr1: pd.Series, arr2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2):
        s = 0
        index1 = 0
        index2 = 0
        len1 = len(ar1)
        len2 = len(ar2)
        while True:
            if len1 == index1 or len2 == index2:
                break

            if ar1[index1][0] != ar2[index2][0]:
                if ar1[index1][0] > ar2[index2][0]:
                    index2 += 1
                else:
                    index1 += 1
            else:
                s += math.e ** (-alpha * abs(ar1[index1][2] - ar2[index2][2]))
                index1 += 1
                index2 += 1

        return float(s)

    return pd.Series([calc_rating(x, y) for x, y in zip(arr1, arr2)])


@pandas_udf(FloatType())
def mfps_p_udf(
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
        s = 1 / s
        return float(s)

    return pd.Series(
        [calc_mfps(rc, ru, rd, rt) for rc, ru, rd, rt in zip(rCom, rUse, rDet, rTim)]
    )


## Rating commodity
temp_df = temp_df.withColumn(
    "rc",
    rating_commodity_p_udf(col("items_ratings_times"), col("items_ratings_times_")),
)

## Rating usefulness
temp_df = temp_df.withColumn(
    "ru",
    rating_usefulness_p_udf(col("items_ratings_times"), col("items_ratings_times_")),
)

## Rating detail
temp_df = (
    temp_df.join(average_df, on="user")
    .join(
        average_df.withColumnRenamed("user", "user_").withColumnRenamed(
            "avg(rating)", "avg_"
        ),
        on="user_",
    )
    .withColumn(
        "rd",
        rating_detail_p_udf(
            col("items_ratings_times"),
            col("items_ratings_times_"),
            col("avg(rating)"),
            col("avg_"),
        ),
    )
)

## Rating time
temp_df = temp_df.withColumn(
    "rt",
    rating_time_p_udf(col("items_ratings_times"), col("items_ratings_times_")),
)

## MFPS
temp_df = temp_df.withColumn(
    "mfps", mfps_p_udf(col("rc"), col("ru"), col("rd"), col("rt"))
)

temp_df.select(["user", "user_", "rc", "ru", "rd", "rt", "mfps"]).show()
