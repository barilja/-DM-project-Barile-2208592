import pandas as pd

# ---------- EXTRACT ----------
# Spotify Top 1000 CSV
spotify_df = pd.read_csv("spotify datasets/spotify_top_1000_tracks.csv")

# IMDb Datasets (TSV files)
titles_df = pd.read_csv("imdb datasets/title infos/title.basics.tsv", sep="\t", na_values="\\N", dtype={"runtimeMinutes": str}, low_memory=False)
titles_df['runtimeMinutes'] = pd.to_numeric(titles_df['runtimeMinutes'], errors='coerce')

ratings_df = pd.read_csv("imdb datasets/ratings/title.ratings.tsv", sep="\t", na_values="\\N")
names_df = pd.read_csv("imdb datasets/actors/name.basics.tsv", sep="\t", na_values="\\N")

# ---------- CLEAN SPOTIFY DATA ----------

# Drop rows with missing essential fields
spotify_df.dropna(subset=['track_name', 'artist', 'release_date', 'duration_min'], inplace=True)

# Filter valid durations
spotify_df = spotify_df[(spotify_df['duration_min'] >= 1) & (spotify_df['duration_min'] <= 15)]

# Normalize key string columns
spotify_df['track_name'] = spotify_df['track_name'].astype(str).str.strip().str.title()
spotify_df['artist'] = spotify_df['artist'].astype(str).str.strip().str.title()
spotify_df['album'] = spotify_df['album'].astype(str).str.strip().str.title()

# Deduplicate: Keep only the most popular version of each track by artist
spotify_df = (
    spotify_df.sort_values(by='popularity', ascending=False)
              .drop_duplicates(subset=['track_name', 'artist'], keep='first')
              .reset_index(drop=True)
)

# ---------- CLEAN IMDB TITLES DATA ----------
titles_df.drop_duplicates(subset=['tconst'], inplace=True)
titles_df['titleType'] = titles_df['titleType'].str.strip().str.lower()
titles_df = titles_df[titles_df['titleType'].isin(['movie', 'tvseries'])]
titles_df['startYear'] = pd.to_numeric(titles_df['startYear'], errors='coerce')
titles_df.dropna(subset=['startYear'], inplace=True)
titles_df = titles_df[titles_df['startYear'] >= 1990]
titles_df['primaryTitle'] = titles_df['primaryTitle'].str.strip().str.title()
titles_df['originalTitle'] = titles_df['originalTitle'].str.strip().str.title()
titles_df['genres'] = titles_df['genres'].fillna("Unknown").str.strip().str.title()

# ---------- CLEAN IMDB RATINGS DATA ----------
ratings_df.drop_duplicates(subset=['tconst'], inplace=True)
ratings_df.dropna(subset=['tconst', 'averageRating', 'numVotes'], inplace=True)
ratings_df['averageRating'] = pd.to_numeric(ratings_df['averageRating'], errors='coerce')
ratings_df = ratings_df[(ratings_df['averageRating'] >= 0) & (ratings_df['averageRating'] <= 10)]
ratings_df['numVotes'] = pd.to_numeric(ratings_df['numVotes'], errors='coerce')
ratings_df = ratings_df[ratings_df['numVotes'] >= 10]

# ---------- CLEAN IMDB NAMES DATA ----------
names_df.drop_duplicates(subset=['nconst'], inplace=True)
valid_roles = ['soundtrack', 'music_artist', 'producer']
def has_valid_profession(profession_str):
    if pd.isna(profession_str):
        return False
    professions = [p.strip().lower() for p in profession_str.split(',')]
    return any(prof in professions for prof in valid_roles)
names_df = names_df[names_df['primaryProfession'].apply(has_valid_profession)]
names_df = names_df[~(names_df['birthYear'].isna() & names_df['deathYear'].notna())]
names_df = names_df[(names_df['birthYear'].isna()) | (names_df['birthYear'] > 1940)]
names_df['primaryName'] = names_df['primaryName'].str.strip().str.title()
names_df['knownForTitles'] = names_df['knownForTitles'].fillna('').str.replace(' ', '')

# ---------- SAVE CLEANED FILES ----------
spotify_df.to_csv("cleaned datasets/cleaned_spotify.csv", index=False)
titles_df.to_csv("cleaned datasets/cleaned_imdb_titles.csv", index=False)
ratings_df.to_csv("cleaned datasets/cleaned_imdb_ratings.csv", index=False)
names_df.to_csv("cleaned datasets/cleaned_imdb_names.csv", index=False)

print("Extraction and cleansing completed.")
