# DROP TABLES
songplay_table_drop = ("""DROP TABLE IF EXISTS songplays""")
user_table_drop = ("""DROP TABLE IF EXISTS users""")
song_table_drop = ("""DROP TABLE IF EXISTS songs""")
artist_table_drop = ("""DROP TABLE IF EXISTS artists""")
time_table_drop = ("""DROP TABLE IF EXISTS time""")

# CREATE TABLES
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                              (songplay_id SERIAL PRIMARY KEY, \
                              start_time bigint REFERENCES time(start_time) NOT NULL, \
                              user_id int, \
                              level varchar, \
                              song_id varchar REFERENCES songs(song_id), \
                              artist_id varchar REFERENCES artists(artist_id), \
                              session_id int, location varchar, user_agent text); """)

# user_id, first_name, last_name, gender, level
user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (user_id int PRIMARY KEY, \
                        first_name varchar, \
                        last_name varchar, \
                        gender char, \
                        level varchar);""")

# song_id, title, artist_id, year, duration
song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (song_id text PRIMARY KEY, \
                        title text, \
                        artist_id text NOT NULL, \
                        year int, duration double precision);""")


# artist_id, name, location, latitude, longitude
artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
                          (artist_id varchar PRIMARY KEY, \
                           name text, \
                           location text, \
                           latitude real, \
                           longitude real);""")

# start_time, hour, day, week, month, year, weekday
time_table_create = ("""CREATE TABLE IF NOT EXISTS time 
                        (start_time bigint PRIMARY KEY, \
                        hour int, day int, week int, \
                        month int, year int, \
                        weekday int);""")

# INSERT RECORDS
# Let use auto index
songplay_table_insert = ("""INSERT INTO songplays \
                            (start_time, user_id, \
                             level, song_id, artist_id, \
                             session_id, location, user_agent) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ;""")


# Note: user_id is primary key
user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (user_id) DO UPDATE \
                          SET level=EXCLUDED.level;""")

# Note: song_id is primary key
song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) \
                        VALUES (%s, %s, %s, %s, %s) \
                        ON CONFLICT (song_id) DO NOTHING;""")

# Note: artist_id is primary key
artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) \
                          VALUES (%s, %s, %s, %s, %s) \
                          ON CONFLICT (artist_id) DO UPDATE \
                          SET artist_id=EXCLUDED.artist_id;""")

# Note: start_time is primary key
time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) \
                        VALUES (%s, %s, %s, %s, %s, %s, %s) \
                        ON CONFLICT (start_time) DO UPDATE \
                        SET start_time=EXCLUDED.start_time;""")

# FIND SONGS
song_select = ("""SELECT songs.song_id, \
                         artists.artist_id \
                  FROM songs JOIN artists ON \
                  songs.artist_id = artists.artist_id
                  WHERE (%s = songs.title) AND \
                        (%s = artists.name) AND
                        (%s = songs.duration)""")

# QUERY LISTS
create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]