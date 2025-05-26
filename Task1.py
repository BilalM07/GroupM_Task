import kaggle
import pandas as pd
import sqlite3

def download_and_extract_kaggle_dataset(dataset_name, download_path):
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files(dataset_name, path=download_path, unzip=True)
    ''' Ideally, I could have limited the processing to only the specific files required. 
    However, due to encoding issues in the files, 
    I tried manually specify encodings also using tools such as the chardet Python library and other encoding detection methods. 
    Despite these efforts, the absence of consistent input data prevented me from accurately determining the correct file encoding. 
    As a result, I opted to download and extract the complete dataset instead, as the unzip process defaults all files to UTF-8 encoding'''

def load_dataframes(albums_path, tracks_path):
    albums_df = pd.read_csv(albums_path)
    tracks_df = pd.read_csv(tracks_path)
    return albums_df, tracks_df

def clean_albums_df(albums_df):
    albums_df.drop_duplicates(inplace=True)
    albums_df.dropna(subset=['track_id'], inplace=True)
    albums_df.drop([
        'track_number', 'album_type', 'album_name', 'album_popularity', 'album_id',
        'total_tracks', 'artists', 'artist_id', 'artist_0', 'artist_1', 'artist_2',
        'artist_3', 'artist_4', 'artist_5', 'artist_6', 'artist_7', 'artist_8',
        'artist_9', 'artist_10', 'artist_11', 'duration_sec'
    ], axis=1,inplace=True)
    ''' While track_number, album-related information, and artist details can be valuable for deeper analysis or future use cases, 
    they are not required for the current task. 
    Specifically, the artist_0 to artist_11 columns could ideally be consolidated into a single column for cleaner representation,
    and the duration_sec column is redundant since duration_ms provides a more precise measurement. 
    However, given that none of these columns are relevant to our immediate objective, 
    they are being dropped at this stage for simplicity and performance optimization. '''
    return albums_df

def transform_and_merge(tracks_df, albums_df):
    tracks_df.rename(columns={'id': 'track_id'}, inplace=True)
    tracks_df = tracks_df[
        (tracks_df['explicit'] == False) &
        (tracks_df['track_popularity'] > 50)
    ].reset_index(drop=True)
    merged_df = pd.merge(tracks_df, albums_df, on='track_id', how='left')
    merged_df['radio_mix'] = merged_df['duration_ms'] <= (3 * 60 * 1000)
    return merged_df

def save_to_sqlite(df, db_path, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, index=False, if_exists='replace')
    return conn

def execute_and_print(conn, query):
    print(pd.read_sql(query, conn))
    

def main():
    dataset_name = 'tonygordonjr/spotify-dataset-2023'
    download_path = 'datasets/kaggle'
    albums_path = f"{download_path}/spotify-albums_data_2023.csv"
    tracks_path = f"{download_path}/spotify_tracks_data_2023.csv"
    db_path = 'spotify_cleaned_data.db'
    table_name = 'spotify_cleaned_data'
    query_top_labels = f"""
    SELECT label, COUNT(*) AS total_tracks
    FROM {table_name}
    GROUP BY label
    ORDER BY total_tracks DESC
    LIMIT 20; 
    """
    query_top_tracks = f"""
    WITH ranked_tracks AS (
    SELECT
    track_id,track_name,track_popularity,release_date,ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY track_popularity DESC ) AS rn
    FROM {table_name}
    WHERE release_date BETWEEN '2020-01-01' AND '2023-12-31')
    SELECT track_name,track_id,track_popularity,release_date
    FROM ranked_tracks
    WHERE rn = 1
    ORDER BY track_popularity DESC
    LIMIT 25; 
    """
    
    download_and_extract_kaggle_dataset(dataset_name, download_path)
    albums_df, tracks_df = load_dataframes(albums_path, tracks_path)
    albums_df = clean_albums_df(albums_df)
    merged_df = transform_and_merge(tracks_df, albums_df)

    del tracks_df
    del albums_df
    
    conn = save_to_sqlite(merged_df, db_path, table_name)

    execute_and_print(conn, query_top_labels)
    execute_and_print(conn, query_top_tracks)

    conn.close()

if __name__ == "__main__":
    main()