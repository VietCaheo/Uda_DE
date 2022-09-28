1. Project Summary:
#-----------------
Base on Data Modeling on Postgres, build an ETL pipeline using Python. To complete the project:
    1. Define a fact and dimensions tables for a star schema for a particular analystic focus
    2. Write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQLs

##############################################################
2. How to run Python Scripts:
    1. To run py files: just put to Jupiter Note book as syntax "%run <file_name>.py"
    2. Order to run:
        -> run the create_tables.py firstly
        -> run etl.ipynb  for check a sigle log file or run whole data-set by running elt.py
        -> run test.ipynb for check result
    Note: each time finished test or debug, It should be reset kernal, or shutdown the files for next turn test.
##############################################################
3. Database Design:
    - Applied schema with a Fact table (songplays) and 4 Dimesion table includes (time; uses; songs; artists)
        + songtables : is a Fact table combined information about song, artist, and customer who has experience with the song. Table includes: unique PrimaryKey call song_id, ForeignKey for linking to another tables: start_time; user_id; song_id; artist_id
        + time: a table provide information about timestamp of song recording, also provide anther 6 columns for another needed time format (extract from timestamp).
        + users: provide information of customer who has listened the songs.
        + songs: provide some basic info of a song, also a foreign key that used to connect o artists table
        + artists: provide some basic info of an artist.
    - ETL Process:
        + Provide some helpful API to get data files path in a destination folders.
        + For each type of data file: song_data or log_data: ETL loop over for read all json data file.
        + On each time read a single data file: Raw data will be extract, transform to appropriate format (e.g Datafram, List, Serires...) then Load into corresponding tables.
    - Project Reposite files: 
        + create_tables.py: 
            -> Establish connection database to host
            -> Provide a compact way to do CREAT TABLE and DROP TABLE statement, loop over for all QUERRY in  the project.
        +sql_queries.py:
            -> Make lists for creat tables and drop tables; implement detail CREAT TABLE, INSERT TABLE statement
        +etl.ipynb: 
            -> detail implement ETL for a single data file
        + etl.py: 
            -> fullt implementation ETL for whole data files.
        + test.ipynb: 
            -> like a testing tool: for verification all above works.
##############################################################
4. Referring lists:
#---------------
for convert numpy.dt64:
https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64

to fix issue "violates foreign key constraint" , and could not insert songid, arstistid = None value to table songplay_table
https://knowledge.udacity.com/questions/651840


issue when run Sanity test:
%load_ext sql

"Environment variable $DATABASE_URL not set, and no connect string given.
Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: dict_keys([])"
