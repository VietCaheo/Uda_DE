from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
%matplotlib inline
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Exericise _ Viet") \
    .getOrCreate()

path = "data/sparkify_log_small.json"
df = spark.read.json(path)

#  Data Exploration
df.take(5)

# Question 1: Which page did user id "" NOT visit
df.printSchema()

# filter for users with blank user_id
blank_pages = df.filter(df.userId == '') \
    .select(col('page') \
    .alias('blank_pages')) \
    .dropDuplicates()

# get a list of possible pages that could be visited
all_pages = df.select('page').dropDuplicates()

# find values in all_pages that are not in blank_pages
# these are the pages that the blank user did not go to
for row in set(all_pages.collect()) - set(blank_pages.collect()):
    print(row.page)



# Question 2: typing...


#Question 3: How many female users do we have in the data set?
df.filter(df.gender == 'F') \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count()

# Question 4: How many songs were played from the most played artist?
df.filter(df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Artistcount') \
    .sort(desc('Artistcount')) \
    .show(1)

#Question 5:  How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
function = udf(lambda ishome : int(ishome == 'Home'), IntegerType())
user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

cusum = df.filter((df.page == 'NextSong') | (df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', function(col('page'))) \
    .withColumn('period', Fsum('homevisit').over(user_window))

cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}).show()

# Question 5: Another solution by using pandas and numpy only
data = cusum.toPandas
#print out the table
data.head()

data = data[data.page == 'NextSong']
data.shape

data.period.sum()

data.period.mean()

data.groupby(['userID', 'period'])['period'].count().mean()

