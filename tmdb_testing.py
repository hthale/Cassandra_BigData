import time
import uuid
import pandas as pd
from cassandra.cluster import Cluster
import numpy as np

# Verbindung zu einem einzelnen Cassandra-Knoten herstellen
cluster = Cluster(['127.0.0.1'])  # IP-Adresse des Knoten, hier für einen lokalen Knoten
session = cluster.connect()

# Keyspace und Tabelle erstellen
keyspace = 'tmdb'
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")

session.set_keyspace(keyspace)

# Tabelle erstellen (falls nicht vorhanden)
session.execute("""
    CREATE TABLE IF NOT EXISTS movies (
        id UUID PRIMARY KEY,
        budget INT,
        genres TEXT,
        homepage TEXT,
        movie_id INT,
        keywords TEXT,
        original_language TEXT,
        original_title TEXT,
        overview TEXT,
        popularity FLOAT,
        production_companies TEXT,
        production_countries TEXT,
        release_date TEXT,
        revenue INT,
        runtime INT,
        spoken_languages TEXT,
        status TEXT,
        tagline TEXT,
        title TEXT,
        vote_average FLOAT,
        vote_count INT
    );
""")

# Lade die CSV-Datei
csv_file = "tmdb_5000_movies.csv"  # Passe den Pfad an
df = pd.read_csv(csv_file)

# Funktion zum Importieren der Daten und Messung der Zeit
def import_data():
    query = """
    INSERT INTO movies (id, budget, genres, homepage, movie_id, keywords, original_language, original_title, overview, popularity,
                        production_companies, production_countries, release_date, revenue, runtime, spoken_languages, status, tagline, title, vote_average, vote_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    start_time = time.time()

    # Daten in Cassandra einfügen
    for _, row in df.iterrows():
        # Wir stellen sicher, dass leere oder nicht vorhandene Felder als None behandelt werden und NaN-Werte durch None ersetzt werden
        session.execute(query, (
            uuid.uuid4(),  # Generiere eine UUID für jede Zeile
            row.get('budget', None) if not pd.isna(row.get('budget')) else None,  # handle NaN and empty values
            row.get('genres', None) if not pd.isna(row.get('genres')) else None,  # handle NaN and empty values
            row.get('homepage', None) if not pd.isna(row.get('homepage')) else None,  # handle NaN and empty values
            row.get('id', None) if not pd.isna(row.get('id')) else None,  # handle NaN and empty values
            row.get('keywords', None) if not pd.isna(row.get('keywords')) else None,  # handle NaN and empty values
            row.get('original_language', None) if not pd.isna(row.get('original_language')) else None,  # handle NaN and empty values
            row.get('original_title', None) if not pd.isna(row.get('original_title')) else None,  # handle NaN and empty values
            row.get('overview', None) if not pd.isna(row.get('overview')) else None,  # handle NaN and empty values
            row.get('popularity', None) if not pd.isna(row.get('popularity')) else None,  # handle NaN and empty values
            row.get('production_companies', None) if not pd.isna(row.get('production_companies')) else None,  # handle NaN and empty values
            row.get('production_countries', None) if not pd.isna(row.get('production_countries')) else None,  # handle NaN and empty values
            row.get('release_date', None) if not pd.isna(row.get('release_date')) else None,  # handle NaN and empty values
            row.get('revenue', None) if not pd.isna(row.get('revenue')) else None,  # handle NaN and empty values
            row.get('runtime', None) if not pd.isna(row.get('runtime')) else None,  # handle NaN and empty values
            row.get('spoken_languages', None) if not pd.isna(row.get('spoken_languages')) else None,  # handle NaN and empty values
            row.get('status', None) if not pd.isna(row.get('status')) else None,  # handle NaN and empty values
            row.get('tagline', None) if not pd.isna(row.get('tagline')) else None,  # handle NaN and empty values
            row.get('title', None) if not pd.isna(row.get('title')) else None,  # handle NaN and empty values
            row.get('vote_average', None) if not pd.isna(row.get('vote_average')) else None,  # handle NaN and empty values
            row.get('vote_count', None) if not pd.isna(row.get('vote_count')) else None  # handle NaN and empty values
        ))

    end_time = time.time()
    return end_time - start_time

# Daten importieren
print("Importiere Daten...")
try:
    time_taken = import_data()
    print(f"Importdauer für {len(df)} Einträge: {time_taken:.2f} Sekunden")
except Exception as e:
    print(f"Fehler beim Importieren der Daten: {e}")

# Abfrage der ersten 10 Einträge aus der Cassandra-Datenbank
print(" Erste 10 Einträge aus der Cassandra-Datenbank:")
rows = session.execute("SELECT * FROM movies LIMIT 10;")
for row in rows:
    print(row)
