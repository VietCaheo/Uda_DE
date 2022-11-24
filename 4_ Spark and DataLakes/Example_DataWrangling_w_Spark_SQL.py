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

# 
spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()

# Get the json data file and read into dataframe
path = "data/sparkify_log_small.json"
user_log = spark.read.json(path)

# Take a row of dataframe user_log
user_log.take(1)

# to print all columns' name
user_log.printSchema()

#---------
""" Create a View And Run Queries: temporary view is actually a table """
#---------
# Creats a temporary view againts which you can run SQL queries
user_log.createOrReplaceTempView("user_log_table")

spark.sql("SELECT *FROM user_log_table LIMIT 2").show()

# way2 to display query
spark.sql('''
            SELECT user_log_table
            LIMIT 2
            ''').show()

spark.sql('''
            SELECT COUNT(*)
            FROM user_log_table
            ''').show()

spark.sql('''
            SELECT userID, firstname, PaGe, song
            FROM user_log_table
            WHERE userID == '1046'
            ''').collect()

# to print out all distinct value of column `page`
spark.sql('''
            SELECT DISTINCT page
            FROM user_log_table
            ORDER BY page ASC
            ''').show()

#------------
""" User Defined Functions """

# Using lambda function syntax to define a function to get hour from ts
spark.usd.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x/1000.0).hour))
#------------
spark.sql('''
            SELECT *, get_hour(ts) AS hour
            FROM user_log_table
            LIMIT 1
        ''').collect()

songs_in_hour = spark.sql('''
                            SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
                            FROM user_log_table
                            WHERE page = "NextSong"
                            GROUP BY hour
                            ORDER BY cast(hour as int) ASC
                            ''')
# Show the above query
songs_in_hour.show()

""" Convert Results to Pandas """
songs_in_hour_pd = songs_in_hour.toPandas()

print(songs_in_hour_pd)


