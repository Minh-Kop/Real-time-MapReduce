import pyspark, os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when, collect_list, coalesce, concat, udf
from pyspark.sql.types import DoubleType, ArrayType
from pyspark import SparkConf
import math

# file_path = './spark/test1.csv'

# spark = SparkSession.builder.appName('spark_clustering').getOrCreate()
# new_df = spark.read.csv('file://' + os.path.abspath(file_path), header=True, inferSchema=True).show()

input_file_path = './input/input_file.txt'
item_file_path = './input/items.txt'
spark_config = SparkConf().setAppName('spark_clustering').set('spark.driver.memory','4g')


spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

def write_to_file(df, file_name):
    df.coalesce(1).write.mode("overwrite").csv("file://" + os.path.abspath(file_name), header=True)

def euclidean_distance(arr1, arr2):
    return math.sqrt(sum((x-y)**2 for x,y in zip(arr1, arr2)))

### read input
# read input file
pd_input_df = pd.read_csv(input_file_path, sep='\t', names=['user-item','rating-time'])
pd_input_df[['user','item']] = pd_input_df['user-item'].str.split(';', expand=True)
pd_input_df[['rating','time']] = pd_input_df['rating-time'].str.split(';', expand=True)
pd_input_df = pd_input_df.drop(['user-item','rating-time','time'], axis=1)
pd_input_df['rating'] = pd_input_df['rating'].astype(float)

# read item file
pd_item_df = pd.read_csv(item_file_path, sep='\t', names=['item', 'categories']).astype(str)
spark_item_df = spark.createDataFrame(pd_item_df)

# drop time, merge categories into input
pd_input_df = pd.merge(pd_input_df, pd_item_df, on='item')

# transfer from pandas to spark DataFrame
spark_input_df = spark.createDataFrame(pd_input_df)
# spark_input_df.printSchema()

### Clustering
number_of_clusters = 3
multiplier = 10

## chi2
# calculate user average
temp_df = spark_input_df.groupBy('user').mean('rating')

# create a list full user-item matrix
temp_df = temp_df.crossJoin(spark_item_df)

# join observed into full matrix
temp_df = temp_df.join(spark_input_df, on=['user', 'item', 'categories'], how='left')

# add average for all null value
temp_df = temp_df.withColumn("rating", when(col("rating").isNull(), col("avg(rating)")).otherwise(col("rating")))

# drop avg
fill_matrix_df = temp_df.drop('avg(rating)')

# observe value
observed_df = fill_matrix_df.groupBy(['user','categories']).sum('rating')
# observed_df = observed_df.orderBy(['user','categories'])

# categories probability
temp_df = fill_matrix_df.groupBy('categories').count()
temp_df = temp_df.withColumn('count', col('count') / fill_matrix_df.count())

# sum user rating
sum_rating_df = fill_matrix_df.groupBy('user').sum('rating')

# expected value
expected_df = sum_rating_df.crossJoin(temp_df)
expected_df = expected_df.withColumn("E", col('count')*col('sum(rating)'))
expected_df = expected_df.drop('sum(rating)')
# expected_df = expected_df.orderBy(['user','categories'])

# Chi2
temp_df = expected_df.join(observed_df, on=['user', 'categories'])
temp_df = temp_df.withColumn("temp", ((col("E")-col('sum(rating)'))**2)/col("E"))
temp_df = temp_df.groupBy('user').sum('temp').drop(*['count', 'E', 'sum(rating)'])
# chi2_df = chi2_df.orderBy('user')

## Get initial centroids
# Get k*a user with highest chi2
temp_df = temp_df.orderBy(col('sum(temp)').desc()).limit(number_of_clusters * multiplier)
temp_df = temp_df.join(fill_matrix_df, on='user').drop(*['sum(temp)','categories']).orderBy(['user','item'])

## calculate distance between each centroid
# get all rating in a array
array_df = temp_df.drop('item').groupBy('user').agg(collect_list('rating').alias('ratings')).orderBy('user')

# choose first line as first initial user
init_centroids_df = array_df.limit(1)

# get all remaining clusters
for i in range(number_of_clusters - 1):
    # cross joint and filter to get all user pair with that user
    array_df = array_df.join(init_centroids_df, on='user', how='leftanti')
    temp_df = array_df.crossJoin(init_centroids_df.limit(1).select([col('user').alias('user_'),col('ratings').alias('ratings_')]))

    # register the euclidean distance udf
    euclidean_distance_udf = udf(euclidean_distance, DoubleType())

    # calculate distance
    temp_df = temp_df.withColumn("distance", euclidean_distance_udf(col('ratings'),col("ratings_"))).drop(*['ratings','ratings_']).orderBy(col('distance').desc())

    # add highest to the initial centroids
    temp_df = temp_df.limit(1).join(array_df, on='user')
    temp_df = temp_df.select('user', 'ratings')

    init_centroids_df = temp_df.union(init_centroids_df)

init_centroids_df.show()