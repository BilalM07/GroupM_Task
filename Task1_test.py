
import pandas as pd
from Task1 import clean_albums_df, transform_and_merge

def test_clean_albums_df_removes_unneeded_columns():
    sample_df = pd.DataFrame({
        'track_name': ['Test Track'],
        'release_date':['015-06-09 00:00:00 UTC'],
        'label':['Test Label'],
        'track_id': ['1'],
        'track_number': [1],
        'album_type': ['album'],
        'duration_sec': [120],
        'duration_ms': [120000],
        'album_name': ['Test Album'],
        'album_popularity': [60],
        'album_id': ['alb1'],
        'total_tracks': [10],
        'artists': ['Artist A'],
        'artist_id': ['art1'],
        'artist_0': ['Artist A'],
        'artist_1': [None],
        'artist_2': [None],
        'artist_3': [None],
        'artist_4': [None],
        'artist_5': [None],
        'artist_6': [None],
        'artist_7': [None],
        'artist_8': [None],
        'artist_9': [None],
        'artist_10': [None],
        'artist_11': [None]
    })
    cleaned_df = clean_albums_df(sample_df.copy())
    print (cleaned_df.columns)
    assert 'track_name' in cleaned_df.columns  
    assert 'release_date' in cleaned_df.columns
    assert 'label' in cleaned_df.columns
    assert 'track_id' in cleaned_df.columns
    assert 'duration_ms' in cleaned_df.columns
# all important columns should remain after cleaning.

def test_transform_and_merge_filters_correctly():
    tracks_df = pd.DataFrame({
        'id': ['1', '2'],
        'track_popularity': [80, 40],
        'explicit': [False, True]
    })
    albums_df = pd.DataFrame({
        'track_name': ['Test Track'],
        'release_date':['015-06-09 00:00:00 UTC'],
        'label':['Test Label'],
        'track_id': ['1'],
        'duration_sec': [180],
        'duration_ms': [180000]
    })
    result = transform_and_merge(tracks_df.copy(), albums_df.copy())
    assert len(result) == 1  # Only one track should pass the filters
    assert result.iloc[0]['radio_mix'] == True # Radio mix is True for duration <= 3 minutes
    assert result.iloc[0]['explicit'] == False # Explicit should be False

def main():
    test_clean_albums_df_removes_unneeded_columns()
    test_transform_and_merge_filters_correctly()
    print('Testing complete. Modules working as expected.') # This line is for confirmation, in case of failiure asssertions will raise errors.

if __name__ == "__main__":
    main()