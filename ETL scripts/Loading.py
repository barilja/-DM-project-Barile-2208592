import psycopg2
import pandas as pd

# === DATABASE CONNECTION CONFIG ===
conn = psycopg2.connect(
    dbname="DM Project",
    user="Jacob",
    password="DM Project",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

# === CLEAR TABLES IN FOREIGN KEY ORDER ===
try:
    print("\n🧹 Clearing existing data from all tables...")
    cur.execute("DELETE FROM fact_track_title;")
    cur.execute("DELETE FROM bridge_genre;")
    cur.execute("DELETE FROM bridge_profession;")
    cur.execute("DELETE FROM dim_genre;")
    cur.execute("DELETE FROM dim_profession;")
    cur.execute("DELETE FROM dim_track;")
    cur.execute("DELETE FROM dim_title;")
    cur.execute("DELETE FROM dim_artist;")
    conn.commit()
    print("✅ All tables cleared.\n")
except Exception as e:
    conn.rollback()
    print(f"❌ Failed to clear tables: {e}")
    cur.close()
    conn.close()
    exit(1)

# === HELPER FUNCTION ===
def load_csv_to_table(filename, insert_sql, table_name):
    df = pd.read_csv(filename)

    print(f"\n🧾 Columns found in {table_name}:", df.columns.tolist())

    # === Normalize fields as needed ===
    if table_name == "dim_title":
        def convert_boolean(val):
            try:
                return bool(int(float(val)))
            except:
                return None
        df['IsAdult'] = df['IsAdult'].apply(convert_boolean)

    if table_name == "dim_track":
        def normalize_date(val):
            try:
                val = str(val)
                if len(val) == 4:
                    return f"{val}-01-01"
                pd.to_datetime(val)  # validate format
                return val
            except:
                return None
        df['ReleaseDate'] = df['ReleaseDate'].apply(normalize_date)

    if table_name == "fact_track_title":
        def clean_key(val):
            try:
                return str(int(float(val)))
            except:
                return None
        df['KeyTrack'] = df['KeyTrack'].apply(clean_key)
        df['KeyTitle'] = df['KeyTitle'].apply(str)  # Keep tconst format
        df['KeyArtist'] = df['KeyArtist'].apply(clean_key)

    # === Insert data ===
    print(f"\n📥 Inserting into {table_name}...")
    try:
        for i, row in df.iterrows():
            cur.execute(insert_sql, tuple(row))
        conn.commit()
        print(f"✅ Finished {table_name} with no errors.\n")
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Insertion failed for {table_name}. Transaction rolled back.\nError: {e}\n")
        return False

# === DIMENSION TABLES ===

if load_csv_to_table(
    "dimensions datasets/dim_artist.csv",
    """
    INSERT INTO dim_artist (artist_name, birth_year, death_year, key_artist)
    VALUES (%s, %s, %s, %s)
    """,
    "dim_artist"
):
    if load_csv_to_table(
        "dimensions datasets/dim_track.csv",
        """
        INSERT INTO dim_track (track_name, duration_minutes, spotify_url, album, release_date, key_artist, key_track)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        "dim_track"
    ):
        if load_csv_to_table(
            "dimensions datasets/dim_title.csv",
            """
            INSERT INTO dim_title (key_title, title, original_title, is_adult, runtime_minutes, type, start_year, end_year, key_artist)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            "dim_title"
        ):
            if load_csv_to_table(
                "dimensions datasets/dim_profession.csv",
                """
                INSERT INTO dim_profession (profession, key_profession)
                VALUES (%s, %s)
                """,
                "dim_profession"
            ):
                if load_csv_to_table(
                    "dimensions datasets/dim_genre.csv",
                    """
                    INSERT INTO dim_genre (genre, key_genre)
                    VALUES (%s, %s)
                    """,
                    "dim_genre"
                ):
                    # === BRIDGE TABLES ===
                    if load_csv_to_table(
                        "dimensions datasets/bridge_profession.csv",
                        """
                        INSERT INTO bridge_profession (key_artist, key_profession)
                        VALUES (%s, %s)
                        """,
                        "bridge_profession"
                    ):
                        if load_csv_to_table(
                            "dimensions datasets/bridge_genre.csv",
                            """
                            INSERT INTO bridge_genre (key_title, key_genre)
                            VALUES (%s, %s)
                            """,
                            "bridge_genre"
                        ):
                            # === FACT TABLE ===
                            load_csv_to_table(
                                "dimensions datasets/track_title_fact.csv",
                                """
                                INSERT INTO fact_track_title (key_track, key_title, key_artist, popularity, average_voting, number_of_votes, fuzzy_match)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """,
                                "fact_track_title"
                            )

# === CLOSE CONNECTION ===
cur.close()
conn.close()
