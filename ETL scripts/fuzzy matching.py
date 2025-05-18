import pandas as pd
import re
from rapidfuzz import fuzz

# ---------- LOAD CLEANED DATA ----------
spotify_df = pd.read_csv("cleaned datasets/cleaned_spotify.csv")
titles_with_ratings_df = pd.read_csv("joined datasets/joined_titles_with_ratings.csv")
names_titles_df = pd.read_csv("joined datasets/joined_names_titles_ratings.csv")

# ---------- NORMALIZE COLUMNS ----------
for col in ['track_name', 'artist', 'album']:
    spotify_df[col] = spotify_df[col].astype(str).str.strip().str.title()

for col in ['primaryName', 'primaryTitle', 'originalTitle']:
    names_titles_df[col] = names_titles_df[col].astype(str).str.strip().str.title()

titles_with_ratings_df['primaryTitle'] = titles_with_ratings_df['primaryTitle'].astype(str).str.strip().str.title()
titles_with_ratings_df['originalTitle'] = titles_with_ratings_df['originalTitle'].astype(str).str.strip().str.title()

# ---------- BASIC CLEAN FUNCTION ----------
def clean(text):
    return re.sub(r'[^a-zA-Z0-9 ]', '', str(text)).strip().lower()

# ---------- PREPARE FOR ARTIST-BASED MERGE ----------
spotify_df['artist_clean'] = spotify_df['artist'].apply(clean)
names_titles_df['primaryName_clean'] = names_titles_df['primaryName'].apply(clean)

# Looser match on cleaned artist name
artist_merge = pd.merge(spotify_df, names_titles_df, left_on='artist_clean', right_on='primaryName_clean', how='inner')

# ---------- INCLUSION-BASED MATCH ----------
artist_merge['track_name_lc'] = artist_merge['track_name'].str.lower()
artist_merge['album_lc'] = artist_merge['album'].str.lower()
artist_merge['primaryTitle_lc'] = artist_merge['primaryTitle'].str.lower()
artist_merge['originalTitle_lc'] = artist_merge['originalTitle'].str.lower()

inclusion_matched = artist_merge[
    artist_merge.apply(
        lambda row: (
            row['primaryTitle_lc'] in row['track_name_lc'] or
            row['primaryTitle_lc'] in row['album_lc'] or
            row['originalTitle_lc'] in row['track_name_lc'] or
            row['originalTitle_lc'] in row['album_lc']
        ),
        axis=1
    )
].copy()

inclusion_matched['match_type'] = 'inclusion'

# Remove temporal mismatches
inclusion_matched['release_date'] = pd.to_datetime(inclusion_matched['release_date'], errors='coerce')
inclusion_matched['startYear'] = pd.to_numeric(inclusion_matched['startYear'], errors='coerce')
inclusion_matched = inclusion_matched[inclusion_matched['release_date'].dt.year <= inclusion_matched['startYear']]

# Clean up
inclusion_matched.drop(columns=[
    'track_name_lc', 'album_lc', 'primaryTitle_lc', 'originalTitle_lc'
], inplace=True)

# ---------- FUZZY MATCH ----------
# Clean necessary columns for fuzzy scoring
artist_merge['track_name_clean'] = artist_merge['track_name'].apply(clean)
artist_merge['album_clean'] = artist_merge['album'].apply(clean)
artist_merge['primaryTitle_clean'] = artist_merge['primaryTitle'].apply(clean)
artist_merge['originalTitle_clean'] = artist_merge['originalTitle'].apply(clean)

def fuzzy_score(row):
    return max([
        fuzz.token_set_ratio(row['track_name_clean'], row['primaryTitle_clean']),
        fuzz.token_set_ratio(row['track_name_clean'], row['originalTitle_clean']),
        fuzz.partial_ratio(row['track_name_clean'], row['primaryTitle_clean']),
        fuzz.partial_ratio(row['track_name_clean'], row['originalTitle_clean'])
    ])

artist_merge['fuzzy_score'] = artist_merge.apply(fuzzy_score, axis=1)

# Apply tighter threshold for precision
fuzzy_matched = artist_merge[artist_merge['fuzzy_score'] >= 50].copy()
fuzzy_matched['match_type'] = 'fuzzy'

# Remove temporal mismatches
fuzzy_matched['release_date'] = pd.to_datetime(fuzzy_matched['release_date'], errors='coerce')
fuzzy_matched['startYear'] = pd.to_numeric(fuzzy_matched['startYear'], errors='coerce')
fuzzy_matched = fuzzy_matched[fuzzy_matched['release_date'].dt.year <= fuzzy_matched['startYear']]

def confidence(score):
    if score >= 70: return "high"
    elif score >= 60: return "medium"
    else: return "low"

fuzzy_matched['confidence'] = fuzzy_matched['fuzzy_score'].apply(confidence)

# Clean up
fuzzy_matched.drop(columns=[
    'track_name_clean', 'album_clean', 'primaryTitle_clean', 'originalTitle_clean'
], inplace=True)

# ---------- REMOVE DUPLICATES ----------
match_keys = ['track_name', 'artist', 'primaryTitle']
inclusion_keys = inclusion_matched[match_keys].apply(tuple, axis=1)
fuzzy_matched = fuzzy_matched[~fuzzy_matched[match_keys].apply(tuple, axis=1).isin(inclusion_keys)]

# ---------- FINAL UNION ----------
final_matches = pd.concat([inclusion_matched, fuzzy_matched], ignore_index=True)

# Drop unmatched or invalid rows
final_matches.dropna(subset=[
    'release_date', 'startYear', 'track_name', 'primaryName', 'tconst', 'averageRating', 'numVotes'
], inplace=True)

# Select required columns for compatibility with the transformation script
required_columns = [
    'track_name', 'primaryName', 'tconst', 'popularity',
    'averageRating', 'numVotes'
]

# Include fuzzy_score if available
if 'fuzzy_score' in final_matches.columns:
    required_columns.append('fuzzy_score')

final_matches = final_matches[required_columns].drop_duplicates()

# ---------- SAVE ----------
final_matches.to_csv("fuzzy datasets/spotify_imdb_matched_filtered.csv", index=False)
print(f"✅ Inclusion matches: {len(inclusion_matched)}")
print(f"✅ Fuzzy matches:     {len(fuzzy_matched)}")
print(f"🎯 Total final matches written: {len(final_matches)}")
