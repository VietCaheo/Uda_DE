import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):
    # open song file
    # to read_json a song. the parameter `filepath` is a sigle
    df = pd.read_json(filepath, lines=True)

    # insert song record
    # get needed-columns for song table
    df_song=df[['song_id', 'title', 'artist_id', 'year', 'duration']]

    # each song_data represented for a song
    df_song_1 = df_song.values[0]
    song_data = df_song_1.tolist()

    # insert each table (for a song) to song_talbe -> conn.commit() for all tables later
    cur.execute(song_table_insert, song_data)

    # insert artist record
    # get columns for artist table
    df_artist=df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]

    df_artist_1 = df_artist.values[0]
    artist_data = df_artist_1.tolist()

    #insert each table for artist -> conn.commit() for all tables later
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    # get whole frame with raw timestamp columns (upto millisecons)
    t = df[['ts']]
    ts_dtime = pd.to_datetime(t.values[0])

    # get serires type for next steps
    ts_dtime_S = pd.Series(ts_dtime)

    # insert time data records
    # extract from ts for individual time slicing
    time_data = []
    time_data.append(ts_dtime_S[0])
    time_data.append(ts_dtime_S.dt.hour[0])
    time_data.append(ts_dtime_S.dt.day[0])
    time_data.append(ts_dtime_S.dt.weekofyear[0])
    time_data.append(ts_dtime_S.dt.month[0])
    time_data.append(ts_dtime_S.dt.year[0])
    time_data.append(ts_dtime_S.dt.weekday[0])

    # columns using for time_table
    column_labels = ['ts','hour','day','weekofyear','month','year','weekday']

    dict_time=dict(zip(column_labels,time_data))
    time_df = pd.DataFrame(dict_time,index=['row0'])

    # original Uda-template code
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # columns for user_table
    df_user=df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    df_user_1 = df_user.values[0]
    user_data = df_user_1.tolist()

    user_df =

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()