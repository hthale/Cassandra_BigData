import time
import uuid
import pandas as pd
from cassandra.cluster import Cluster

# Verbindung zum Cassandra-Cluster herstellen
cluster = Cluster(['localhost'])  # Lokaler Host
session = cluster.connect()

# Keyspace erstellen, falls er noch nicht existiert
keyspace = 'neo_data'
session.set_keyspace(keyspace)

# Sekundärindizes für häufig gefilterte Spalten erstellen
session.execute("""
    CREATE INDEX IF NOT EXISTS ON asteroids (est_diameter_min);
""")
session.execute("""
    CREATE INDEX IF NOT EXISTS ON asteroids (est_diameter_max );
""")

# CSV-Datei laden
csv_file = "neo.csv"
df = pd.read_csv(csv_file)

# Funktion zum Importieren der Daten
def import_data():
    query = """
    INSERT INTO asteroids (id, name, est_diameter_min, est_diameter_max, relative_velocity, miss_distance, orbiting_body, sentry_object, absolute_magnitude, hazardous)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    start_time = time.time()

    # Durch alle Zeilen im DataFrame iterieren und die Daten in Cassandra einfügen
    for _, row in df.iterrows():
        session.execute(query, (
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

    end_time = time.time()
    return end_time - start_time

# Daten importieren und Dauer anzeigen
print("Importiere Daten...")
time_taken = import_data()
print(f"Importdauer für {len(df)} Einträge: {time_taken:.2f} Sekunden")

