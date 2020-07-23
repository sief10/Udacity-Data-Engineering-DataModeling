import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]]
    song_data=song_data.sort_values("song_id")
    song_data.drop_duplicates(subset ="song_id", keep = 'first', inplace = True) 
    cur.executemany(song_table_insert,song_data.values)

    
    # insert artist record
    artist_data = df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]]
    artist_data=artist_data.sort_values("artist_id")
    artist_data.drop_duplicates(subset ="artist_id", keep = 'first', inplace = True) 
    cur.executemany(artist_table_insert,artist_data.values)



def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df =  df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t =  df[["ts"]]
    
    # insert time data records
    time_df=t.copy()
    time_df["TIMESTAMP"]= pd.to_datetime(time_df['ts'],unit='ms')
    time_df["YEAR"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).year
    time_df["MONTH"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).month
    time_df["WEEK"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).week
    time_df["DAY"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).day
    time_df["HOUR"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).hour
    time_df["WEEKDAY"]=pd.DatetimeIndex(time_df["TIMESTAMP"]).day_name()
    time_df=time_df.drop(columns=["ts"])
    
    time_df=time_df.sort_values("TIMESTAMP")
    time_df.drop_duplicates(subset ="TIMESTAMP", keep = 'first', inplace = True) 

    cur.executemany(time_table_insert,time_df.values)

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]].copy()
    user_df.sort_values("userId")
    user_df.drop_duplicates("userId",inplace=True,keep='last')

    # insert user records
    cur.executemany(user_table_insert,user_df.values)   

    # insert songplay records
    
    ## propagate STG Table 
    df_stg=df[["ts","sessionId","userAgent","song","artist","length","userId","level"]].copy()
    df_stg["START_TIME"]= pd.to_datetime(df_stg.loc[:,'ts'],unit='ms')
    df_stg=df_stg.drop(columns="ts")
    
    cur.executemany(stg_table_insert,df_stg.values)


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
        ## insert into Fact Table
        cur.execute(songplay_table_insert)
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