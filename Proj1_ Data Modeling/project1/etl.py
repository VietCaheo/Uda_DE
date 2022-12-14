import os
import glob
import psycopg2
import pandas as pd
import numpy as np
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):
    """
    Function to load a single json song_data file and extract to DataFrame , then load into tables
    """
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
    """
    Function to load a single json log_data file and extract to DataFrame , then load into tables
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # convert timestamp column to datetime
    # get whole frame with raw timestamp columns (upto millisecons)
    rawTime = df[['ts']]

    #first row ts and convert to datetime in pandas
    ts_dtime_S = []

    # convert whole item in rawTime df
    for index, row in rawTime.iterrows():
    #    count += 1
        ts_dtime = pd.to_datetime(row["ts"])
        ts_dtime_S.append(pd.Series(ts_dtime))

    # insert time data records
    # extract from ts for individual time slicing
    time_data = []
    time_data_e = []

    for ele in ts_dtime_S:
        time_data_e.append(np.datetime64(ele.values[0]).astype(datetime))
        time_data_e.append(ele.dt.hour.values[0])
        time_data_e.append(ele.dt.day.values[0])
        time_data_e.append(ele.dt.weekofyear.values[0])
        time_data_e.append(ele.dt.month.values[0])
        time_data_e.append(ele.dt.year.values[0])
        time_data_e.append(ele.dt.weekday.values[0])
        time_data.append(time_data_e)
        time_data_e = []

    # for debuging:
    #print(time_data_e)
    #print()
    #print(time_data)

    column_labels = ['ts','hour','day','weekofyear','month','year','weekday']

    arr = np.array(time_data)
    time_df = pd.DataFrame(arr,columns=column_labels)
    #time_df.head()

    # insert each time row in time table into the time_table
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    # columns for user_table
    df_user=df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    df_user_1 = df_user.values[0]
    user_data = df_user_1.tolist()

    user_column=['userId', 'firstName', 'lastName', 'gender', 'level']
    user_dict=dict(zip(user_column,user_data))

    user_df = pd.DataFrame(user_dict,index=['row0'])

    # Insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records
    # To filtering criterial points: song's title; artist and length with songplay_table
    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record
        # Except songid, artistid were get from result, another get from df that read from a Json file
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]

        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    """
    Function for helping get all json data file in folders,
        and provide a loop over whole data files in same kind.
    """

    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process
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