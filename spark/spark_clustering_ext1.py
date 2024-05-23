import pyspark, os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when

# file_path = './spark/test1.csv'

# spark = SparkSession.builder.appName('spark_clustering').getOrCreate()
# new_df = spark.read.csv('file://' + os.path.abspath(file_path), header=True, inferSchema=True).show()

input_file_path = './input/input_file.txt'
item_file_path = './input/items.txt'

spark = SparkSession.builder.appName('spaek_clustering').getOrCreate()

def write_ro_file(df, file_name):
    df.coalesce(1).write.mode("overwrite").csv("file://" + os.path.abspath(file_name), header=True)


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
spark_input_df.printSchema()

### Clustering
noCluster = 3
multiplier = 10

## chi2
# calculate user average
user_avg_df = spark_input_df.groupBy('user').mean('rating')

# create a list full user-item matrix
matrix_df = user_avg_df.crossJoin(spark_item_df)

# join observed into full matrix
matrix_df = matrix_df.join(spark_input_df, on=['user', 'item', 'categories'], how='left')

# add average for all null value
full_matrix_df = matrix_df.withColumn("rating", when(col("rating").isNull(), col("avg(rating)")).otherwise(col("rating")))

# drop avg
fill_matrix_df = full_matrix_df.drop('avg(rating)')

# observe value
observed_df = fill_matrix_df.groupBy(['user','categories']).sum('rating')
observed_df = observed_df.orderBy(['user','categories'])

# categories probability
cate_prob_df = fill_matrix_df.groupBy('categories').count()
cate_prob_df = cate_prob_df.withColumn('count', col('count') / fill_matrix_df.count())

# sum user rating
sum_rating_df = fill_matrix_df.groupBy('user').sum('rating')

# expected value
expected_df = sum_rating_df.crossJoin(cate_prob_df)
expected_df = expected_df.withColumn("E", col('count')*col('sum(rating)'))
expected_df = expected_df.drop('sum(rating)')
expected_df = expected_df.orderBy(['user','categories'])

# Chi2
chi2_df = expected_df.join(observed_df, on=['user', 'categories'])
chi2_df = chi2_df.withColumn("temp", ((col("E")-col('sum(rating)'))**2)/col("E"))
chi2_df = chi2_df.groupBy('user').sum('temp').drop(*['count', 'E', 'sum(rating)'])
chi2_df = chi2_df.orderBy('user')

### 

