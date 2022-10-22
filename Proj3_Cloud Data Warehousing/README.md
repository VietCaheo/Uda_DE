Project2: Cloud Data Warehousing
Move to cloud as you work with large amounts of data

Introduction:
Building an ELT pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transform data into a set of dimensional tables.
 It's able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

Project Description:
    Apply Data warehouse and AWS knowledge to build ETL pipeline for a database that hosted on Redshift.
    Need to load data from S3 to staging tables on Redshift and execute SQL statements that creat the analytics tables from these staging tables

Datasets:
    -> There are two datasets that reside in S3:
        Song data: `s3://udacity-dend/song_data`
        Log data: `s3://udacity-dend/log_data`
            (Log data json path : s3://udacity-dend/log_json_path.json)

Schema for Song Play Analysis:
Fact table:
`songplays`: records in event data associated with song plays
	columns: `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`

Dimension Tables:
`users`
`songs`
`artists`
`time`

Project template files:
	creat_table.py
	etl.py
	sql_queries.py

