import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import io


def process_song_file(conn, cur, filepath):
    """
    - reads song JSON from given filepath into pandas DataFrame
    - connects to sparkify DB and writes values to songs and artists tables
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    # dropping duplicates before insert to improve speed
    artist_columns = ['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']
    artist_data = df[artist_columns].drop_duplicates().values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(conn, cur, filepath):
    """
    - reads log JSON from given file path into pandas DataFrame
    - filters data, applies datetime formatting to timestamp column
    - bulk inserts records into time table
    - inserts records into users table
    - bulk inserts records into songsplays table

    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    t = df['ts']
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')

    for i in range(len(time_data)):
        time_data[i].rename(column_labels[i], inplace=True)

    # dropping duplicates before bulk insert to improve speed
    time_df = pd.concat(time_data, axis=1).drop_duplicates()
    
    # use COPY for quicker bulk insert
    output = io.StringIO()
    time_df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'time')

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for index, row in user_df.drop_duplicates().iterrows():
        user_data = (row.userId, row.firstName, row.lastName, row.gender, row.level)
        cur.execute(user_table_insert, user_data)
    
    # insert songplay records
    
    # get song and artist details from database, write to DataFrame
    artists_songs = pd.read_sql_query(select_artists_songs, conn)
    
    # merge the songplay log data with artists_songs to get song_id and artist_id fields
    df2 = df.merge(artists_songs, how='left', 
                    left_on=['song', 'artist', 'length'],
                    right_on=['song_name', 'artist_name', 'duration'])

    # limit to fields on interest for insert
    songplays_columns = ['ts', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent']
    songplays_df= df2[songplays_columns]

    # used COPY for bulk insert into songplays table
    output = io.StringIO()
    songplays_df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_expert(songplays_table_copy, output)


def process_data(cur, conn, filepath, func):
    """
    - iterates through a given filepath
    - implements respective song and log functions define above
    """
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
        func(conn, cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - establishes connection to Postgres sparkifydb
    - executes processing for song files and log files defined above
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()