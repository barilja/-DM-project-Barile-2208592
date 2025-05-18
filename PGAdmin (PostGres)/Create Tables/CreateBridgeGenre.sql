CREATE TABLE bridge_genre (
    key_title TEXT REFERENCES dim_title(key_title) NOT NULL,
    key_genre TEXT REFERENCES dim_genre(key_genre) NOT NULL,
    PRIMARY KEY (key_title, key_genre)
);
