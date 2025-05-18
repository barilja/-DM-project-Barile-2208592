CREATE TABLE dim_track (
    key_track TEXT PRIMARY KEY,
    track_name TEXT NOT NULL,
    duration_minutes double precision NOT NULL,
    spotify_url TEXT NOT NULL,
    album TEXT NOT NULL,
    release_date DATE NOT NULL,
    key_artist TEXT REFERENCES dim_artist(key_artist) NOT NULL
);
