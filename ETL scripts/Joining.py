import pandas as pd
# ---------- JOIN ----------

# Load datasets
spotify_df = pd.read_csv("cleaned datasets/cleaned_spotify.csv")
titles_df = pd.read_csv("cleaned datasets/cleaned_imdb_titles.csv")
ratings_df = pd.read_csv("cleaned datasets/cleaned_imdb_ratings.csv")
names_df = pd.read_csv("cleaned datasets/cleaned_imdb_names.csv")

# Join titles with ratings
titles_with_ratings_df = pd.merge(titles_df, ratings_df, on='tconst', how='inner')

# Explode knownForTitles in the names dataset to one row per (name, title)
names_df_exploded = names_df.copy()
names_df_exploded['knownForTitles'] = names_df_exploded['knownForTitles'].str.split(',')
names_df_exploded = names_df_exploded.explode('knownForTitles')
names_df_exploded.rename(columns={'knownForTitles': 'tconst'}, inplace=True)

# Join names with titles_with_ratings on tconst
names_titles_df = pd.merge(names_df_exploded, titles_with_ratings_df, on='tconst', how='inner')

# ---------- DIRECT JOIN ON ARTIST AND PRIMARY NAME ----------

# Join Spotify dataset with IMDb names/titles on artist_name == primaryName
spotify_imdb_joined_df = pd.merge(spotify_df, names_titles_df, left_on='artist', right_on='primaryName', how='inner')

# ---------- SAVE JOINED FILES ----------
titles_with_ratings_df.to_csv("joined datasets/joined_titles_with_ratings.csv", index=False)
names_titles_df.to_csv("joined datasets/joined_names_titles_ratings.csv", index=False)
spotify_imdb_joined_df.to_csv("joined datasets/joined_spotify_imdb.csv", index=False)

print("Join complete!")