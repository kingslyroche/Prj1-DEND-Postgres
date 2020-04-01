import os
import glob
import psycopg2
import pandas as pd
import csv
from sql_queries import *


def process_song_file(cur, filepath):
    
    '''
    Function desc: Process song_data: Tables loaded -> songs and artists
    
    Parameters:
        
    cur -> cursor variable to postgres
    
    filepath -> path of the songs files
    
    '''
    
    
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[["song_id","title","artist_id","year","duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[["artist_id","artist_name","artist_location","artist_latitude","artist_longitude"]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    
    
    '''
    Function desc: Process log_data: Tables loaded -> users and time
    
    parameters:
    
    cur -> cursor variable to postgres
    
    filepath -> path of the log files
    
    '''
    
    df = pd.read_json(filepath,lines=True)
    df = df[df["page"]=="NextSong"]
    t =  pd.to_datetime(df["ts"],unit='ms')
    
    # insert time data records
    time_data = ([x, x.hour, x.day, x.week, x.month, x.year, x.dayofweek] for x in t)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.filter(["userId","firstName","lastName","gender","level"]).drop_duplicates()
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # process songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [pd.to_datetime(row.ts,unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        #cur.execute(songplay_table_insert, songplay_data)
        with open('songplay.csv', 'a') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(songplay_data)
    
    

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
       
        print('{}/{} files processed.'.format(i, num_files))


def main():
    
    
     #Initialize connection to local postgres server and set cursor.
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    
    conn.set_session(autocommit=True)

    cur = conn.cursor()
    
    
    '''
    Check and removes the songplay.csv file.
    
    Songplay.csv is created by the process_log_file function and 
    
    finally copied (COPY) into songplay table instead of running
    
    individual insert statements
    
    '''
    
    if os.path.exists("/home/workspace/songplay.csv"):
            os.remove("/home/workspace/songplay.csv")
            print("songplay csv removed!")
    else:
            print("The file does not exist")
            
    
   
    #Process song_data: Tables -> songs and artists
    
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    
    #Process song_data: Tables -> users and time table / File -> songplay.csv
    
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    #Load Songplay table from processed csv file directly
    cur.execute(""" COPY songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) FROM '/home/workspace/songplay.csv' WITH CSV """)
    
    conn.close()


if __name__ == "__main__":
    main()