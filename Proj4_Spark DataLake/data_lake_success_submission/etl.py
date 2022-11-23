import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['IAM_ACCESS_KEY']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['IAM_ACCESS_KEY']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file

    # local test
    # song_data = input_data

    # load with small amount of json data files; 
    # small sample :'song_data/A/B/C/*.json'  
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)
    
    #[!] with json file, automatic infer schema when read.json file. But for reading csv file might be different
    print("to see the original df read from json song data \n")
    df.printSchema()
    df.show(5)

    # extract dimensional table `songs_table`: df.select(), return a new dataframe from original one `df`
    print("extract from root schema to be  dimensional table songs_table \n")
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year', \
                            'duration')
    songs_table.printSchema()
    songs_table.show(5)
    
    # write songs table to parquet files partitioned by year and artist
    # save df songs_table as Parquet files, maintaining the schema information
    # parquet(output_data+'songplay/')
    songs_table.write \
               .partitionBy("year", "artist_id") \
               .mode('overwrite') \
               .parquet(output_data+'songs_table/')
    print("Finished write parquet files for songs_table ++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

    # extract columns to create artists table
    print("<###################################################################### \n")
    print("extract from root schema to be dimensional table artists_table \n")
    artists_table = df.select('artist_id', \
                              col('artist_name').alias('name'), \
                              col('artist_location').alias('location'), \
                              col('artist_latitude').alias('latitude'), \
                              col('artist_longitude').alias('longitude'))
    artists_table.printSchema()
    artists_table.show(5)
    
    # write artists table to parquet files
    # partition by name and artist_id
    artists_table.write \
                 .partitionBy("name", "artist_id") \
                 .mode('overwrite') \
                 .parquet(output_data+'artists_table/')
    
    print("Finished write parquet files for artists_table ++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    
    # test with local data files
    # log_data = input_data

    # test with S3
    log_data = os.path.join(input_data, 'log_data/2018/11/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # # filter by actions for song plays: by page= NextSong
    df = df.filter(df['page']=='NextSong')
    
    # drop any ele with at least NA, without dropna(), there are a lot of rows with null will be returned
    df = df.dropna()
    
    print("+++++++++#########+++++++++#########+++++++++#########+++++++++#########+++++++++#########+++++++++#########\n")
    print("to see the original df schema of log data \n")
    df.printSchema()
    df.show(5)

    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    print("extract from root schema to be  dimensional table songs_table \n")
    # TO handle the duplicate records. Use "SELECT DISTINCT" in spark.sql or drop_duplicates() on the data frame.
    users_table = users_table.drop_duplicates(subset=['userId'])

    users_table = df.select(col('userId').alias('user_id'), \
                            col('firstName').alias('first_name'), \
                            col('lastName').alias('last_name'), \
                            'gender', \
                            'level')

    users_table.printSchema()
    users_table.show(5)

    # write users table to parquet files
    users_table.write \
               .partitionBy("user_id") \
               .mode('overwrite') \
               .parquet(output_data+'users_table/')

    print("Finished write parquet files for users_table ++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

    # For TimeTable creating
    # raw table provide ts as epoch timestamps, need to convert to datetime_type (supported by spark)
    # create timestamp column from original timestamp column with interval 1s as start_time
    get_timestamp = udf(lambda x:  datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    
    # extract the column start_time
    df = df.withColumn("start_time", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    # remaining columns of timetable will be used by `pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format`
    # extract columns to create time table
    time_table = df.select('start_time', \
                              hour(col("start_time")).alias('hour'), \
                             date_format(col("datetime"), 'd').alias('day'), \
                             weekofyear(col("datetime")).alias('week'), \
                             month(col("datetime")).alias('month'), \
                             year(col("datetime")).alias('year'), \
                              weekofyear(col("datetime")).alias('weekday')
                          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write \
              .mode('overwrite') \
              .partitionBy('year', 'month') \
              .parquet(output_data+'time_table/')
    print("Finished write parquet files for time_table ++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
    
    ####################################################################################
    # read in song data to use for songplays table
    input_data_song_clone = 'data/song_data/A/A/A/*.json'
    song_df = spark.read.json(input_data_song_clone)
    
    #test see song_data inside log_data process
    print("see song_data gain inside log_data process \n")
    song_df.printSchema()
    song_df.show(3)

    # just for easy debug marking log_df is log dataframe
    log_df = df

    two_table = log_df.join(song_df, \
                            (log_df.song == song_df.title) \
                            & (log_df.artist == song_df.artist_name) \
                            & (log_df.length == song_df.duration), \
                            'left_outer')
    
    # Add songplay_id by monotonically_increasing_id() method
    two_table = two_table.withColumn('songplay_id', monotonically_increasing_id())

    #Creat songplays_table
    # alias made inside df or song_df, is not exist yet
    songplays_table = two_table.select('songplay_id', \
                                        col("datetime").alias('start_time'), \
                                        col("userId").alias('user_id'), \
                                        col("level"), \
                                        song_df.song_id, \
                                        song_df.artist_id, \
                                        col("sessionId").alias('session_id'), \
                                        col("location"), \
                                        col("userAgent").alias('user_agent'))

    # Add more `year` , `month` prior- to creat parquet files
    songplays_table = songplays_table.withColumn('year', year('start_time'))\
                                      .withColumn('month', month('start_time'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write\
                   .mode('overwrite')\
                   .partitionBy('year', 'month')\
                   .parquet(output_data+'songplays_table/')

    print("Finished write parquet files for songplays_table ++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"

    # to staging parquets file of each table into S3 
    output_data = "s3a://viet-datalake-bucket/"
    
    # for local test lonly
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
