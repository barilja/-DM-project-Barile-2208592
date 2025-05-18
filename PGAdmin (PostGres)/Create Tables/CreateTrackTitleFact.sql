CREATE TABLE fact_track_title (
    key_track TEXT REFERENCES dim_track(key_track) NOT NULL,
    key_title TEXT REFERENCES dim_title(key_title) NOT NULL,
    key_artist TEXT REFERENCES dim_artist(key_artist) NOT NULL,
    popularity INT NOT NULL,
    average_voting double precision NOT NULL,
    number_of_votes double precision NULL,
    fuzzy_match double precision,
    PRIMARY KEY (key_track, key_title,key_artist)
);
