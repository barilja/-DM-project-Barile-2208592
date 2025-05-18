CREATE TABLE dim_artist (
    key_artist TEXT PRIMARY KEY,
    artist_name TEXT NOT NULL,
    birth_year DOUBLE precision NOT NULL,
    death_year DOUBLE precision
);
