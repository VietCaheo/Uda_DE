def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    # Here for test read local json files only
    log_data = input_data

    # read log data file
    df = spark.read.json(log_data)
    
    # filter whole read df by page= NextSong
    df = df.filter(df['page']=='NextSong')
    
    # drop any ele with at least NA, without dropna(), there are a lot of rows with null will be returned
    df = df.dropna()
    
    print("+++++####+++++####+++++####+++++####+++++####+++++####+++++####+++++####+++++####+++++####\n")
    print("to see the original df schema of log data \n")
    df.printSchema()
    df.show(5)
    
    # filter by actions for song plays
    #df = 
    
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    print("extract from root schema to be  dimensional table songs_table \n")
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
               .parquet("users.parquet")

     # read back parquet files after saving above
    parquetUsersFile = spark.read.parquet("users.parquet")
    
    # creat tempo parquet view
    print("<###################################################################### \n")
    parquetUsersFile.createOrReplaceTempView("parquetUsersFile")

    print("to see users parque view, partitioned by name and user_id \n")
    spark.sql("""
              SELECT *
              FROM parquetUsersFile
              ORDER BY user_id
              LIMIT 5""").show()
    
    # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
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
              .parquet("time_table.parquet")
    
    # Just for see new time_table schema
    # read back parquet files after saving above
    parquetTimeTableFile = spark.read.parquet("time_table.parquet")
    
    # creat tempo parquet view
    print("<###################################################################### \n")
    parquetTimeTableFile.createOrReplaceTempView("parquetTimeTableFile")

    print("to see timetable parque view, partitioned by `year` and `month` \n")
    spark.sql("""
              SELECT *
              FROM parquetTimeTableFile
              ORDER BY start_time
              LIMIT 5""").show()
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # read in song data to use for songplays table
    input_data_song_clone = 'data/song_data/A/A/A/*.json'
    song_df = spark.read.json(input_data_song_clone)
    
    #test see song_data inside log_data process
    print("see song_data gain inside log_data process \n")
    song_df.printSchema()
    song_df.show(5)
    
    # use pyspark.sql.functions.row_number() to make songplay_id
    # extract columns from joined song and log datasets to create songplays table 
    
    # Ex df have added 02 columns `start_time` and `datetime` already
    
    songplays_table = df.join(song_df, \
                             (df.song == song_df.title) &\
                             (df.artist == song_df.artist_name) &\
                             (df.length == song_df.duration))
                              
    # Add songplay_id by monotonically_increasing_id() method
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    
    #>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
    # alias made inside df or song_df, is not exist yet
    songplays_table = songplays_table.select('songplay_id', \
                                             'start_time', \
                                             'userId', \
                                             'level', \
                                             'song_id', \
                                             'artist_id', \
                                             'sessionId', \
                                             'location', \
                                             'userAgent', \
                                             month(col("datetime")).alias('month'), \
                                             year(col("datetime")).alias('year') )

    # Add more `year` , `month` prior- to creat parquet files
#     songplays_table = songplays_table.withColumn('year',year('start_time')) \
#                                      .withColumn('month',month('start_time'))
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write \
                   .mode('overwrite') \
                   .partitionBy('year', 'month') \
                   .parquet("songplays_table.parquet")
    
    
    # Just for see new time_table schema
    # read back parquet files after saving above
    parquetSongPlaysFile = spark.read.parquet("songplays_table.parquet")

    print("<###################################################################### \n")
    
     # creat tempo songsplay parquet view
#     parquetSongPlaysFile.createOrReplaceTempView("parquetSongPlaysFile")

#     print("to see songsplay parque view, partitioned by `year` and `month` \n")
#     spark.sql("""
#               SELECT *
#               FROM parquetSongPlaysFile
#               ORDER BY month
#               LIMIT 5""").show()