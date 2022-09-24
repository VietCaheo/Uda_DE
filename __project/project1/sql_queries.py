# DROP TABLES

songplay_table_drop = "DROP table songplays"
user_table_drop = "DROP table user"
song_table_drop = "DROP table songs"
artist_table_drop = "DROP table artists"
time_table_drop = "DROP table time"

# CREATE TABLES

#songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
# [!] datatype regcollation may use for `song_id` and `artist_id`
songplay_table_create = ("CREATE TABLE IF NOT EXISTS songplays  (songplay_id int, start_time timestamp, user_id int, level varchar, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent text);")

# user_id, first_name, last_name, gender, level
# note: `genre` may more than 1 character
user_table_create = ("CREATE TABLE IF NOT EXISTS user (user_id int, first_name varchar, last_name varchar, gender char, level varchar);")

# song_id, title, artist_id, year, duration
# `song_id` see note above
# real is `Single-precision floating-point format`
song_table_create = ("CREATE TABLE IF NOT EXISTS songs (song_id varchar, title text, artist_id varchar, year int, duration real);")

# artist_id, name, location, latitude, longitude
# note: `real` is enough for latitude and longtitude or not
artist_table_create = ("CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name text, location text, latitude real, longitude real);")

# start_time, hour, day, week, month, year, weekday
time_table_create = ("CREATE TABLE IF NOT EXISTS time (start_time timestamp, hour int, day int, week int, month int, year int, weekday int);")

# INSERT RECORDS

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]