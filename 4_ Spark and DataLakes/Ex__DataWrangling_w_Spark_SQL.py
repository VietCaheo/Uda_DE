""" Using SparkSQL instead dataframe"""
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

#  Initial a SparkSession 
spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()

# Read in the data set 
path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)

# Take a row of dataframe user_log
user_log.take(1)

# to print all columns' name
user_log.printSchema()

# Creats a temporary view againts which you can run SQL queries
user_log.createOrReplaceTempView("user_log_table")

# Q1: Which page did userID= empty string
spark.sql(''' 
            SELECT DISTINCT page 
            FROM user_log_table
            WHERE userID==""
        ''').show()

# Q2 Comapraring between using Spark-dataframe vs Spark-SQL


# Q3: How many female users do we have in the data set
spark.sql(''' 
            SELECT COUNT(*)
            FROM user_log_table
            WHERE gender=="F"
            ''').show()


# Q4: How many songs were played from the most played artist
spark.sql(''' 
          SELECT Artist, COUNT(Artist) as c_artist
          FROM user_log_table
          GROUP BY Artist
          ORDER BY c_artist DESC
          ''').show(5)

# Q5: How many songs do users listen to on average between visting our home page ? round up answer to the closet

# using SELECT CASE WHEN 1>0 THEN 1 WHEN 2>0 THEN 2.0 ELSE 1.2 END;
is_home = spark.sql("SELECT userID, page, ts, CASE WHEN page = 'Home' THEN 1 ELSE 0 END AS is_home FROM user_log_table \
            WHERE (page = 'NextSong') or (page = 'Home') \
            ")

# is_home = spark.sql(''' 
#                     SELECT userID, page, ts,
#                     CASE WHEN page == "Home"  THEN 1 ELSE 0 END AS is_home
#                     FROM user_log_table
#                     WHERE page=="NextSong"
#                     OR page=="Home";
#                     ''')
# creat newtempo view from above result
is_home.createOrReplaceTempView("is_home_table")

# sum over the is_home column
cumulative_sum = spark.sql("SELECT *, SUM(is_home) OVER \
                            (PARTITION BY userID \
                            ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) \
                            AS period \
                            FROM is_home_table")

# # sum over the is_home column
# cumulative_sum = spark.sql(''' 
#                             SELECT *, SUM(is_home) OVER 
#                             (
#                             PARTITION BY userID ORDER BY ts DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS period
#                             FROM is_home_table
#                             )
#                             ''')

# keep results in new view
cumulative_sum.createOrReplaceTempView("period_table")


# find the average count for NextSong
spark.sql("SELECT AVG(count_results) FROM \
            (SELECT COUNT(*) AS count_results \
                FROM period_table \
                GROUP BY userID, period, page \
                HAVING page = 'NextSong'\
            ) AS counts"
        ).show()

# find the average count from NextSong
# spark.sql('''SELECT AVG(count_results)
#             FROM 
#             (
#             SELECT COUNT(*) AS count_results
#             FROM period_table
#             GROUP BY userID, period, page
#             HAVING page="NextSong"
#             ) AS counts
#             ''').show()







