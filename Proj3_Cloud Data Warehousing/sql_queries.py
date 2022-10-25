import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# pay much attention for Data_Type
# Some field in log_data and song_data has length different: as `song` and `song>title`
# use bigint for `time`, instead of timestamp
staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events \
                               (artist varchar, \
                                auth varchar, \
                                firstName varchar(50), \
                                gender char, \
                                itemInSession smallint, \
                                lastName varchar(50), \
                                length double precision, \
                                level varchar(8), \
                                location varchar(50), \
                                method varchar(5), \
                                page varchar(30), \
                                registration bigint, \
                                sessionId smallint, \
                                song varchar(220), \
                                status smallint, \
                                ts bigint, \
                                userAgent varchar(200), \
                                userId smallint); """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs \
                            (song_id varchar(18) PRIMARY KEY, \
                            num_songs int, \
                            artist_id varchar(18), \
                            artist_latitude real, \
                            artist_longitude real, \
                            artist_location varchar(220), \
                            artist_name varchar(220), \
                            duration double precision, \
                            title varchar(180), \
                            year smallint); """)
# >>Big boy
songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays \
                              (songplay_id INT IDENTITY(0,1) PRIMARY KEY, \
                              start_time timestamp NOT NULL, \
                              user_id int, \
                              level varchar(8), \
                              song_id varchar(18), \
                              artist_id varchar(18), \
                              session_id int, location varchar(50), user_agent varchar(180)); """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY NOT NULL, \
                        first_name varchar(50), \
                        last_name varchar(50), \
                        gender char, \
                        level varchar(8)); """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id varchar(18) PRIMARY KEY, \
                        title varchar(180), \
                        artist_id varchar(18) NOT NULL, \
                        year smallint, duration double precision); """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar(18) PRIMARY KEY, \
                           artist_name varchar(220), \
                           artist_location varchar(220), \
                           latitude real, \
                           longitude real); """)

# Note: timestamp got from `bigint`
time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (start_time timestamp PRIMARY KEY, \
                        hour int, \
                        day int, \
                        week int, \
                        month int, \
                        year smallint, \
                        weekday smallint); """)

# STAGING TABLES
# should use us-west-2 for the same REGION of S3 that located
# COMPUPDATE OFF: automatic compression is disabled.
ARN_IAM_ROLE = config.get('IAM_ROLE', 'ARN')
LOG_DATA_PATH = config.get('S3','LOG_DATA')
LOG_JSON_PATH = config.get('S3','LOG_JSONPATH')
SONG_DATA_PATH = config.get('S3','SONG_DATA')

staging_events_copy = ("""  COPY staging_events \
                            FROM {} \
                            CREDENTIALS 'aws_iam_role={}' \
                            REGION 'us-west-2' \
                            COMPUPDATE OFF \
                            JSON {} \
                            """).format(LOG_DATA_PATH, ARN_IAM_ROLE, LOG_JSON_PATH)

staging_songs_copy = ("""   COPY staging_songs \
                            FROM {} \
                            CREDENTIALS 'aws_iam_role={}' \
                            REGION 'us-west-2' \
                            COMPUPDATE OFF \
                            JSON 'auto' \
                            """).format(SONG_DATA_PATH, ARN_IAM_ROLE)

# FINAL TABLES
# From `staging_songs_table`, extract directly for song_table and artist_table
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                        SELECT  stg_s.song_id, \
                                stg_s.title, \
                                stg_s.artist_id, \
                                stg_s.year, \
                                stg_s.duration \
                        FROM staging_songs AS stg_s; """)

artist_table_insert = ("""INSERT INTO artists (artist_id, artist_name, artist_location, latitude, longitude) \
                          SELECT DISTINCT stg_s.artist_id, \
                                          stg_s.artist_name, \
                                          stg_s.artist_location, \
                                          stg_s.artist_latitude, \
                                          stg_s.artist_longitude \
                          FROM staging_songs AS stg_s;""")

# Each of user_table: extract only from staging_events table
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                        SELECT DISTINCT stg_e.userId, \
                                        stg_e.firstName, \
                                        stg_e.lastName, \
                                        stg_e.gender, \
                                        stg_e.level \
                        FROM staging_events AS stg_e \
                        WHERE stg_e.userID IS NOT NULL
                        AND stg_e.page='NextSong'; """)

# time_table: Extract from both 
# APPLY Date parts for date or timestamp functions of AWS doc
# Note condition stage_events.page = 'NextSong'. otherwise time_table return with more 82xx rows
time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                         SELECT DISTINCT timestamp 'epoch' + stg_e.ts/1000 * interval '1 second' as start_time, \
                                EXTRACT(hour from start_time), \
                                EXTRACT(day from start_time), \
                                EXTRACT(week from start_time), \
                                EXTRACT(month from start_time), \
                                EXTRACT(year from start_time), \
                                EXTRACT(dayofweek from start_time) \
                        FROM staging_events AS stg_e
                        WHERE stg_e.page='NextSong'; """)

# songplay has been extracted from both `staging_songs_table` and `staging_events_table`
songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, \
                                        session_id, location, user_agent) \
                            SELECT TIMESTAMP WITHOUT TIME ZONE 'epoch' + (stg_e.ts / 1000) * INTERVAL '1 second',
                                    stg_e.userId, \
                                    stg_e.level, \
                                    stg_s.song_id, \
                                    stg_s.artist_id, \
                                    stg_e.sessionId, \
                                    stg_e.location, \
                                    stg_e.userAgent
                            FROM staging_events AS stg_e \
                            JOIN staging_songs AS stg_s \
                            ON stg_e.artist=stg_s.artist_name \
                            AND stg_e.song=stg_s.title \
                            AND stg_e.length=stg_s.duration \
                            WHERE stg_e.page='NextSong'; """)



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create, song_table_create, artist_table_create, user_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]


