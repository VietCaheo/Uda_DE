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
staging_events_table_create= ("""CREAT TABLE IF NOT EXISTS staging_events \
                               (artist varchar(160), \
                                auth varchar(15), \
                                firstName varchar(30), \
                                gender char, \
                                ItemInSession smallint, \
                                lastName varchar(30), \
                                length double precision, \
                                level varchar(8), \
                                location varchar(50), \
                                method varchar(5), \
                                page varchar(15), \
                                registration bigint, \
                                sessionId smallint, \
                                song varchar(100), \
                                status smallint, \
                                ts bigint, \
                                userAgent varchar(180), \
                                userId smallint); """)

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs \
                            (song_id varchar(18) PRIMARY KEY, \
                            num_songs int, \
                            artist_id varchar(18), \
                            artist_latitude real, \
                            artist_longitude real, \
                            artist_location varchar(50), \
                            artist_name varchar(100), \
                            duration double precision, \
                            title varchar(60), \
                            year smallint); """)

songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songplays \
                              (songplay_id SERIAL PRIMARY KEY, \
                              start_time bigint REFERENCES time(start_time) NOT NULL, \
                              user_id int, \
                              level varchar(8), \
                              song_id varchar(18) REFERENCES songs(song_id), \
                              artist_id varchar(18) REFERENCES artists(artist_id), \
                              session_id int, location varchar(50), user_agent varchar(180)); """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY, \
                        first_name varchar(35), \
                        last_name varchar(35), \
                        gender char, \
                        level varchar(8)); """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id varchar(18) PRIMARY KEY, \
                        title varchar(60), \
                        artist_id varchar(18) NOT NULL, \
                        year smallint, duration double precision); """)

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar(18) PRIMARY KEY, \
                           name varchar(100), \
                           location varchar(50), \
                           latitude real, \
                           longitude real); """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (start_time bigint PRIMARY KEY, \
                        hour int, day int, week int, \
                        month int, year smallint, \
                        weekday smallint); """)

# STAGING TABLES
# should use us-west-2 for the same REGION of S3 that located
staging_events_copy = ("""  COPY staging_events \
                            FROM 's3://udacity-dend/log_data' \
                            CREDENTIALS 'aws_iam_role=arn:aws:iam::800432697646:role/myRedshiftRole' \
                            REGION 'us-west-2' \
                            COMPUPDATE OFF \
                            JSON 's3://udacity-dend/log_json_path.json' \
                            """).format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""  COPY staging_songs \
                            FROM 's3://udacity-dend/song_data' \
                            CREDENTIALS 'aws_iam_role=arn:aws:iam::800432697646:role/myRedshiftRole' \
                            REGION 'us-west-2' \
                            COMPUPDATE OFF \
                            JSON 's3://udacity-dend/log_json_path.json' \
                            """).format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays \
                            (start_time, user_id, \
                             level, song_id, artist_id, \
                             session_id, location, user_agent) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ; """)

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (user_id) DO UPDATE \
                          SET level=EXCLUDED.level; """)

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (song_id) DO NOTHING; """)

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (artist_id) DO UPDATE \
                          SET artist_id=EXCLUDED.artist_id; """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT (start_time) DO UPDATE \
                        SET start_time=EXCLUDED.start_time; """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
