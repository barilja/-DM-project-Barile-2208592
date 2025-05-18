CREATE TABLE dim_title (
    key_title TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    original_title TEXT NOT NULL,
    is_adult BOOLEAN NOT NULL,
    runtime_minutes double precision NOT NULL,
    type TEXT NOT NULL,
    start_year double precision NOT NULL,
    end_year double precision,
    key_artist TEXT REFERENCES dim_artist(key_artist) NOT NULL
);
