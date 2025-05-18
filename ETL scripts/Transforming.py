import pandas as pd

# Load the integrated Spotify + IMDb dataset
df = pd.read_csv("joined datasets/joined_spotify_imdb.csv")

# ---------- DIM_ARTIST ----------
# Select the youngest artist per name (based on non-null birth year, fallback to first)
def select_youngest(group):
    non_null = group.dropna(subset=['birthYear'])
    if not non_null.empty:
        return non_null.loc[non_null['birthYear'].idxmax()]
    return group.iloc[0]

# Group safely without DeprecationWarning
artist_raw = df[['primaryName', 'birthYear', 'deathYear']].drop_duplicates()
dim_artist = (
    artist_raw.groupby('primaryName', group_keys=False)
    .apply(select_youngest)
    .reset_index(drop=True)
)

dim_artist.rename(columns={
    'primaryName': 'Artist',
    'birthYear': 'BirthYear',
    'deathYear': 'DeathYear'
}, inplace=True)

dim_artist['KeyArtist'] = dim_artist.index + 1

# Merge KeyArtist back into df BEFORE using it in dim_title
df = df.merge(dim_artist[['Artist', 'KeyArtist']], left_on='primaryName', right_on='Artist', how='left')

# ---------- DIM_TITLE ----------
title_group = df.groupby('tconst').agg({
    'primaryTitle': 'first',
    'originalTitle': 'first',
    'isAdult': 'first',
    'runtimeMinutes': 'first',
    'titleType': 'first',
    'startYear': 'first',
    'endYear': 'first',
    'KeyArtist': 'first'
}).reset_index()

title_group.rename(columns={
    'tconst': 'KeyTitle',
    'primaryTitle': 'Title',
    'originalTitle': 'OriginalTitle',
    'isAdult': 'IsAdult',
    'runtimeMinutes': 'RuntimeMinutes',
    'titleType': 'Type',
    'startYear': 'StartYear',
    'endYear': 'EndYear'
}, inplace=True)

dim_title = title_group

# ---------- DIM_TRACK ----------
dim_track = df[['track_name', 'duration_min', 'spotify_url', 'album', 'release_date', 'KeyArtist']].drop_duplicates().reset_index(drop=True)
dim_track.rename(columns={
    'track_name': 'Track',
    'duration_min': 'DurationMinutes',
    'spotify_url': 'SpotifyURL',
    'album': 'Album',
    'release_date': 'ReleaseDate'
}, inplace=True)
dim_track['KeyTrack'] = dim_track.index + 1

# ---------- DIM_GENRE ----------
genre_exploded = df[['tconst', 'genres']].drop_duplicates()
genre_exploded['genres'] = genre_exploded['genres'].fillna("Unknown")
genre_exploded = genre_exploded.assign(Genre=genre_exploded['genres'].str.split(',')).explode('Genre')
genre_exploded['Genre'] = genre_exploded['Genre'].str.strip().str.title()

dim_genre = pd.DataFrame(genre_exploded['Genre'].unique(), columns=['Genre']).dropna().reset_index(drop=True)
dim_genre['KeyGenre'] = dim_genre.index + 1

# ---------- BRIDGE_GENRE ----------
bridge_genre = genre_exploded.merge(dim_genre, on='Genre', how='left')
bridge_genre = bridge_genre[['tconst', 'KeyGenre']].rename(columns={'tconst': 'KeyTitle'}).drop_duplicates()

# ---------- DIM_PROFESSION ----------
professions_exploded = df[['primaryName', 'primaryProfession']].drop_duplicates()
professions_exploded['primaryProfession'] = professions_exploded['primaryProfession'].fillna('')
professions_exploded = professions_exploded.assign(Profession=professions_exploded['primaryProfession'].str.split(',')).explode('Profession')
professions_exploded['Profession'] = professions_exploded['Profession'].str.strip().str.title()
professions_exploded = professions_exploded[professions_exploded['Profession'] != '']

dim_profession = pd.DataFrame(professions_exploded['Profession'].unique(), columns=['Profession']).dropna().reset_index(drop=True)
dim_profession['KeyProfession'] = dim_profession.index + 1

# ---------- BRIDGE_PROFESSION ----------
bridge_profession = professions_exploded.merge(dim_artist[['Artist', 'KeyArtist']], left_on='primaryName', right_on='Artist', how='left')
bridge_profession = bridge_profession.merge(dim_profession, on='Profession', how='left')
bridge_profession = bridge_profession[['KeyArtist', 'KeyProfession']].drop_duplicates()

# ---------- FACT TABLE: TRACK_TITLE_FACT ----------
fact_df = pd.read_csv("fuzzy datasets/spotify_imdb_matched_filtered.csv")

# Load dimension tables for lookup
dim_artist_lookup = dim_artist[['KeyArtist', 'Artist']]
dim_track_lookup = dim_track[['KeyTrack', 'Track']]
dim_title_lookup = dim_title[['KeyTitle', 'StartYear']]
dim_track_years = dim_track[['KeyTrack', 'ReleaseDate']].copy()
dim_track_years['ReleaseYear'] = pd.to_datetime(dim_track_years['ReleaseDate'], errors='coerce').dt.year

# Merge KeyArtist
fact_df = fact_df.merge(dim_artist_lookup, left_on='primaryName', right_on='Artist', how='left')

# Assign KeyTitle directly from tconst
fact_df['KeyTitle'] = fact_df['tconst']

# Merge KeyTrack
fact_df = fact_df.merge(dim_track_lookup, left_on='track_name', right_on='Track', how='left')

# Drop rows missing foreign keys
fact_df = fact_df.dropna(subset=['KeyArtist', 'KeyTrack'])

# Add release and start year
fact_df = fact_df.merge(dim_track_years[['KeyTrack', 'ReleaseYear']], on='KeyTrack', how='left')
fact_df = fact_df.merge(dim_title_lookup, on='KeyTitle', how='left')

# Enforce: song release year must be ≤ title start year
fact_df = fact_df[fact_df['ReleaseYear'] <= fact_df['StartYear']]

# Select final columns
columns_needed = ['KeyTrack', 'KeyTitle', 'KeyArtist', 'popularity', 'averageRating', 'numVotes']
if 'fuzzy_score' in fact_df.columns:
    columns_needed.append('fuzzy_score')

track_title_fact = fact_df[columns_needed].drop_duplicates()

track_title_fact.rename(columns={
    'popularity': 'TrackPopularity',
    'averageRating': 'TitleAverageRating',
    'numVotes': 'TitleNumVotes',
    'fuzzy_score': 'FuzzyScore'
}, inplace=True)

# ---------- SAVE OUTPUTS ----------
dim_artist.to_csv("dimensions datasets/dim_artist.csv", index=False)
dim_title.to_csv("dimensions datasets/dim_title.csv", index=False)
dim_track.to_csv("dimensions datasets/dim_track.csv", index=False)
dim_genre.to_csv("dimensions datasets/dim_genre.csv", index=False)
bridge_genre.to_csv("dimensions datasets/bridge_genre.csv", index=False)
dim_profession.to_csv("dimensions datasets/dim_profession.csv", index=False)
bridge_profession.to_csv("dimensions datasets/bridge_profession.csv", index=False)
track_title_fact.to_csv("dimensions datasets/track_title_fact.csv", index=False)

print("✅ All dimension and bridge CSVs created successfully.")
print("✅ TrackTitleFact table created and saved.")
