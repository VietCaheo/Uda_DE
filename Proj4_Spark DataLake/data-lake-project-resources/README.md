# Datasets:
    song data: s3://udacity-dend/song_data
    log data: s3://udacity-dend/log_data        --> shall be partitioned by year and month

# Schema for Song Play Anaylytics:
    -> from above datasets: `song` and `log`: need to creat a star schema optimized for queries on song play:
    
    ->[*] Fact Table: `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`
        songplays (data field shall be got from both `log` and `data`)

    ->[*] Dimensional tables:
        users: `user_id, first_name, last_name, gender, level`   <<-- able to get only from log_data
        songs: `song_id, title, artist_id, year, duration`
        artists: `artist_id, name, location, lattitude, longitude`
        time: `start_time, hour, day, week, month, year, weekday`   <<-- get from ts column in log_data

# Implementation:
    -> try test with small dataset in workspace firts
    -> then move on large data set that located in S3


# etl.py: 
    -> reads data from S3
    -> processes that data using Spark
    -> writes them back to S3 as a set of dimensional tables

# Q&A recommend from UDacity:
handling with zip files and large data set in aws S3 bucket:
https://knowledge.udacity.com/questions/96064

##issue with S3 bucket and region of cluster
https://knowledge.udacity.com/questions/907872

### to understand parquet file
https://spark.apache.org/docs/latest/sql-data-sources-parquet.html

### read parquet from S3:
https://knowledge.udacity.com/questions/472251

### timestamp handling;
https://knowledge.udacity.com/questions/192909

### issue when playing with zip files; write output format:
https://knowledge.udacity.com/questions/799167

### write to S3 success
https://knowledge.udacity.com/questions/677823

####
songplays join _1_: https://knowledge.udacity.com/questions/340824





