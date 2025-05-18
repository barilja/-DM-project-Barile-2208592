CREATE TABLE bridge_profession (
    key_artist TEXT REFERENCES dim_artist(key_artist) NOT NULL,
    key_profession TEXT REFERENCES dim_profession(key_profession) NOT NULL,
    PRIMARY KEY (key_artist, key_profession)
);
