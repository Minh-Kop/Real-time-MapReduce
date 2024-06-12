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
    pandas_udf
)
from pyspark.sql.types import DoubleType, ArrayType, NullType, IntegerType
from pyspark import SparkConf
import math


def rating_commodity(arr1, arr2, user):
    if arr1 == None or arr2 == None:
        return 0

    s = 0
    for a, b in zip(arr1, arr2):
        if a != -1 and b != -1:
            s += 1

    if user == '-1':
        print(arr2)

    return int(s)

def rating_usefulness(arr1, arr2, user):
    if arr1 == None or arr2 == None:
        return 0

    s = 0
    for a, b in zip(arr1, arr2):
        if a == -1 and b != -1:
            s += 1
    
    if user == '-1':
        print(arr2)

    return int(s)

def rating_detail_diff_threshold(arr1, arr2, avg1, avg2):
    if arr1 == None or arr2 == None:
        return 0
    s = 0

    for i, (a, b) in enumerate(zip(arr1, arr2)):
        if a != -1 and b != -1:
            if (a < avg1 and b < avg2) or (a > avg1 and b > avg2):
                s += 1
    
    return int(s)

def rating_detail_same_threshold(arr1, arr2, avg):
    if arr1 == None or arr2 == None:
        return 0

    s = 0
    for a, b in zip(arr1, arr2):
        if (a < avg and b < avg) or (a > avg and b > avg):
            s += 1


    return int(s)

def rating_time(arr1, arr2, alpha):
    return float(
        sum(
            [
                math.e ** (-alpha * abs(a - b))
                for a, b in zip(arr1, arr2)
                if (a != -1 and b != -1)
            ]
        )
    )

def mfps(rc, ru, rd, rt):
    if rc != 0:
        rc = 1 / rc
    if ru != 0:
        ru = 1 / ru
    else:
        ru = 1.1
    if rd != 0:
        rd = 1 / rd
    else:
        rd = 1.1
    if rt != 0:
        rt = 1 / rt
    else:
        rt = 1.1

    return float(1 / (1 + rc + ru + rd + rt))


def write_to_file(df, file_name):
    df.coalesce(1).write.csv(
        "file://" + os.path.abspath(f"./spark/{file_name}"), header=True
    )


input_file_path = "./input/input_file.txt"
item_file_path = "./input/items.txt"
spark_config = (
    SparkConf().setAppName("spark_clustering").set("spark.driver.memory", "8g").set("spark.executor.cores", "4").set("spark.driver.maxResultSize", '4g').set("spark.executor.memory", "8g")
)

spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

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
spark_input_df = spark.createDataFrame(pd_input_df)

## Create matrix for smaller join
user_df = (
    spark_input_df.drop(*["item", "rating", "time"])
    .distinct()
    .crossJoin(spark_item_df)
    .orderBy("user")
)
temp_df = (
    user_df.join(spark_input_df, on=["user", "item"], how="left")
    .withColumn("time", col("time").cast("int"))
    .fillna(-1)
)
average_df = (
    spark_input_df.drop("item", "time")
    .groupBy("user")
    .mean("rating")
    .orderBy("user")
)

# average_df = average_df.withColumn('a', concat(col('user'), lit('\t'), col('avg(rating)')))
# average_df.select('a').coalesce(1).write.mode('overwrite').text("file://" + os.path.abspath(f"./spark/avg-file"))

temp_df = (
    temp_df.groupBy("user")
    .agg(collect_list("rating").alias("ratings"), collect_list("time").alias("times"))
    .orderBy("user")
)

# UDF
# rating_commodity_udf = udf(rating_commodity, IntegerType())
# rating_usefulness_udf = udf(rating_usefulness, IntegerType())
# rating_detail_udf = udf(rating_detail_diff_threshold, IntegerType())
# rating_time_udf = udf(rating_time, DoubleType())
# mfps_udf = udf(mfps, DoubleType())
@pandas_udf(IntegerType())
def rating_commodity_p_udf(arr1: pd.Series, arr2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2):
        s = 0
        for x,y in zip(ar1, ar2):
            if x != -1 and y != -1:
                s += 1
        
        return int(s)
    
    return arr1.combine(arr2, lambda x, y: calc_rating(x, y))

@pandas_udf(IntegerType())
def rating_usefulness_p_udf(arr1: pd.Series, arr2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2):
        s = 0
        for x,y in zip(ar1, ar2):
            if x == -1 and y != -1:
                s += 1
     
        return int(s)
    
    return arr1.combine(arr2, lambda x, y: calc_rating(x, y))

@pandas_udf(IntegerType())
def rating_detail_p_udf(arr1: pd.Series, arr2: pd.Series, avg1: pd.Series, avg2: pd.Series, user2: pd.Series) -> pd.Series:
    def calc_rating(ar1, ar2, av1, av2, u2):
        s = 0
        for x,y in zip(ar1,ar2):
            if x != -1 and y != -1:
                if (x > av1 and y > av2) or (x < av1 and y < av2):
                    s += 1

        if u2 == '100':
            index2 = []
            for i, t in enumerate(ar2):
                if t != -1:
                    index2.append(i + 1)
            
            print(index2)   

        return int(s)
    
    return pd.Series([calc_rating(x, y, av1, av2, u2) for x, y, av1, av2, u2 in zip(arr1, arr2, avg1, avg2, user2)])

temp_df = (
    temp_df.crossJoin(
        temp_df.withColumnRenamed("user", "user_")
        .withColumnRenamed("ratings", "ratings_")
        .withColumnRenamed("times", "times_")
    )
    .filter(col("user") != col("user_"))
    .orderBy(["user", "user_"])
)
alpha = 0.000001

# temp_df = temp_df.filter(col('user') == '1')
# print(temp_df.filter(col('user_') == '100').select('ratings_').collect())


## Rating commodity
temp_df = temp_df.withColumn(
    "rc", rating_commodity_p_udf(col("ratings"), col("ratings_"))
)

## Rating usefulness
temp_df = temp_df.withColumn(
    "ru", rating_usefulness_p_udf(col("ratings"), col("ratings_"))
)


## Rating detail
# index2 = []
# u100 = temp_df.filter(col('user_') == '100').select('ratings_').collect()
# for index, t in enumerate(u100[0].ratings_):
#     if t != -1:
#         index2.append(index + 1)
                
# print(index2)


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
            col("ratings"), col("ratings_"), col("avg(rating)"), col("avg_"), col('user_')
        ),
    )
)

temp_df.show()

## Rating time
temp_df = temp_df.withColumn(
    "rt", rating_time_udf(col("times"), col("times_"), lit(alpha))
)

## MFPS
temp_df = temp_df.withColumn(
    "mfps", mfps_udf(col("rc"), col("ru"), col("rd"), col("rt"))
)

temp_df.select(['user','user_','rc','ru','rd','rt','mfps']).show()