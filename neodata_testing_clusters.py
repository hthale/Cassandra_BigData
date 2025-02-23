import time
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Verbindung zum Cassandra-Cluster mit drei Knoten
cluster = Cluster(['xxx.x.x.x','xxx.x.x.x','xxx.x.x.x'])
session = cluster.connect()

# Keyspace erstellen (falls nicht vorhanden) mit SimpleStrategy und Replikationsfaktor 3
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS neo_data
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 3}}; 
""")

# Keyspace auswählen
session.set_keyspace('neo_data')

# Tabelle erstellen (falls nicht vorhanden)
session.execute("""
    CREATE TABLE IF NOT EXISTS asteroids (
        id INT PRIMARY KEY,
        name TEXT,
        est_diameter_min FLOAT,
        est_diameter_max FLOAT,
        relative_velocity FLOAT,
        miss_distance FLOAT,
        orbiting_body TEXT,
        sentry_object BOOLEAN,
        absolute_magnitude FLOAT,
        hazardous BOOLEAN
    );
""")

# CSV-Datei einlesen
csv_file = "neo.csv"
df = pd.read_csv(csv_file)

# Datenimportfunktion mit Batching
def import_data(batch_size=100):  # Batch-Größe auf 100 gesetzt
    query = """
    INSERT INTO asteroids (id, name, est_diameter_min, est_diameter_max, relative_velocity, miss_distance, orbiting_body, sentry_object, absolute_magnitude, hazardous)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)  # Query vorbereiten für bessere Performance

    # Importzeitmessung starten
    start_time = time.time()

    # Batch-Inserts durchführen
    for start in range(0, len(df), batch_size):
        batch = BatchStatement()  # Neuer Batch für jede Gruppe
        for _, row in df.iloc[start:start + batch_size].iterrows():
            batch.add(prepared, (
                row['id'],
                row['name'],
                row['est_diameter_min'],
                row['est_diameter_max'],
                row['relative_velocity'],
                row['miss_distance'],
                row['orbiting_body'],
                row['sentry_object'],
                row['absolute_magnitude'],
                row['hazardous']
            ))
        session.execute(batch)  # Batch ausführen

    # Importzeit messen und zurückgeben
    end_time = time.time()
    return end_time - start_time

print("Importiere Daten...")
time_taken = import_data(batch_size=100)  # Batch-Größe als Parameter
print(f"Importdauer für {len(df)} Einträge: {time_taken:.2f} Sekunden")
