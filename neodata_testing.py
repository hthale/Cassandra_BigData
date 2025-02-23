import time
import uuid
import pandas as pd
from cassandra.cluster import Cluster

cluster = Cluster(['localhost']) #lokaler Host
session = cluster.connect()

keyspace = 'neo_data'
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}; 
""")

session.set_keyspace(keyspace)


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

csv_file = "neo.csv"
df = pd.read_csv(csv_file)

def import_data():
    query = """
    INSERT INTO asteroids (id, name, est_diameter_min, est_diameter_max, relative_velocity, miss_distance, orbiting_body, sentry_object, absolute_magnitude, hazardous)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    start_time = time.time()

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

print("Importiere Daten...")
time_taken = import_data()
print(f"Importdauer für {len(df)} Einträge: {time_taken:.2f} Sekunden")

