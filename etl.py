import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
        This function reads JSON files and read information of song and artist data and saves into song_data and artist_data
        Arguments:
        cur: Database Cursor
        filepath: location of JSON files
        Return: None
    """
    song_files = filepath 
    
 
    df = pd.read_json(filepath,lines=True)
    
    

    
    song_data = df[[ 'song_id','title','artist_id', 'year','duration']]
    song_data = song_data.values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
  
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']] 
    artist_data = artist_data.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

def process_log_file(cur, filepath):
   
    """
            This function reads Log files and reads information of time, user and songplay data and saves into time, user,               songplay
        Arguments:
        cur: Database Cursor
        filepath: location of Log files
        Return: None
    """
        
    log_files =filepath 
   
    df2 = pd.read_json(filepath,lines=True)

    
    df2 = df2[df2['page'] == 'NextSong']

    
    df2['ts'] = pd.to_datetime(df2['ts'] ,unit = 'ms') 
    
    
    time_data = (df2.ts,df2.ts.dt.hour,df2.ts.dt.day,df2.ts.dt.weekofyear,df2.ts.dt.year,df2.ts.dt.month,df2.ts.dt.weekday)
    column_labels = ('start_time','hour','day','week of year','month', 'year',  'weekday')
    test = dict(zip(column_labels,time_data))
    t_df = pd.DataFrame.from_dict(test)


    for i, row in t_df.iterrows():
        
        cur.execute(time_table_insert, list(row))

  
    user_df = df2[['userId','firstName','lastName','gender','level']]
    user_df = user_df.values.tolist()

    
    for row in user_df:
        cur.execute(user_table_insert, row)

    
    for index, row in df2.iterrows():

   
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
    
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

  
        songplay_data = ( row.ts,row.userId , row.level,songid, artistid, row.sessionId,row.location ,  row.userAgent )
        try:
            
            cur.execute(songplay_table_insert, songplay_data)
        except:
            
            print("An exception occurred")

def process_data(cur, conn, filepath, func):
    """
      Process and load data recursively from filepath directory location
    Args:
        cur: psycopg2 cursor
        conn: psycopg2 connection
        filepath: root directory path
        func: function for file processing
    Returns:
        None
    """
    
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

   
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

  
    for i, datafile in enumerate(all_files, 1):
       
        print('{}/{} files processed.'.format(i, num_files))
    
    func(cur, all_files[0])
    conn.commit()
   
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()