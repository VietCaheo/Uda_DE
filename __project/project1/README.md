Base on Data Modeling on Postgres, build an ETL pipeline using Python. To complete the project:
1. Define a fact and dimensions tables for a star schema for a particular analystic focus
2. Write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQLs

##############################################################
# TODO list
-> to read data file in json format: using pandas dataframe : pd.read_json(filepath, lines=true)

-> Schema for Song Play Analysis
Using the song and log datasets, you'll need to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
    songplays - records in log data associated with song plays i.e. records with page NextSong
        songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


Dimension Tables
    users - users in the app
        user_id, first_name, last_name, gender, level
    songs - songs in music database
        song_id, title, artist_id, year, duration
    artists - artists in music database
        artist_id, name, location, latitude, longitude
    time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
##############################################################
# Project Steps:
[!] create_tables.py to run at least once to creat sparkifydb database, which other files connect to

-> Creat tables:
    + sql_queries.py: to creat each table
    + sql_queries.py: to drop each table
    + create_tables.py to creat database and tables
    + run test.ipynb to confirm the creation of tables
    [!] After running make sure click "Restart Kernel" to close connection

-> Build ETL Processes
    Follow instruction to finish etl.ipynb to deveop ETL process for each table.

-> Build ETL Pipeline
    Use what completing in etl.ipynb to comple etl.py (where process for entire datasets). Remember to run creat_tables.py before running etl.py to reset all tables. Run test.ipynb to confirm your records were successfully inserted into each table.

-> Run Sanity Tests




