import time
import csv
from cassandra.cluster import Cluster

# Verbindung zum Cassandra-Cluster herstellen
cluster = Cluster(['localhost'])  # Lokaler Host oder IP-Adresse
session = cluster.connect()

# Keyspace auswählen
keyspace = 'neo_data'
session.set_keyspace(keyspace)


# Helper-Funktion zum Schreiben in eine CSV-Datei
def write_to_csv(filename, rows, headers):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)  # Spaltenüberschriften
        for row in rows:
            writer.writerow(row)
    print(f"Daten wurden in '{filename}' gespeichert!")

# 1. Alle Asteroiden mit einer Mindestgröße größer als 0.01
start_time = time.time()
print("\nExportiere Asteroiden mit einem Mindestdurchmesser > 0.01 in CSV...")
# wenn kein Sekundärindex erstellt wurde (neodata_import_filter_without_allow)
rows = session.execute("""
    SELECT id, name, est_diameter_min, est_diameter_max 
    FROM asteroids 
    WHERE est_diameter_min > 0.01 ALLOW FILTERING;
""")

# wenn Sekundärindex erstellt wurde (neodata_import_filter_without_allow)
# rows = session.execute("""
#     SELECT id, name, est_diameter_min, est_diameter_max 
#     FROM asteroids 
#     WHERE est_diameter_min > 0.01;
# """)


# Daten extrahieren
asteroids_min_size = [(row.id, row.name, row.est_diameter_min, row.est_diameter_max) for row in rows]
write_to_csv('asteroids_min_size.csv', asteroids_min_size, ['id', 'name', 'est_diameter_min', 'est_diameter_max'])
end_time = time.time()
print(f"Anzahl der Asteroiden mit Mindestgröße: {len(asteroids_min_size)}")
print(f"Abfragezeit: {end_time - start_time:.4f} Sekunden")

# 2. Gefährliche Asteroiden anzeigen und exportieren
start_time = time.time()
print("\nExportiere gefährliche Asteroiden in CSV...")
rows = session.execute("""
    SELECT id, name, hazardous 
    FROM asteroids 
    WHERE hazardous = true ALLOW FILTERING;
""")

# Daten extrahieren
hazardous_asteroids = [(row.id, row.name, row.hazardous) for row in rows]
write_to_csv('hazardous_asteroids.csv', hazardous_asteroids, ['id', 'name', 'hazardous'])
end_time = time.time()
print(f"Anzahl der gefährlichen Asteroiden: {len(hazardous_asteroids)}")
print(f"Abfragezeit: {end_time - start_time:.4f} Sekunden")

# 4. Top 10 hellste Asteroiden (sortiert nach absoluter Helligkeit)
start_time = time.time()
print("\nTop 10 hellste Asteroiden:")

# Ohne IS NOT NULL, alle Datensätze abfragen (so performant wie möglich)
rows = session.execute("SELECT id, name, absolute_magnitude FROM asteroids ALLOW FILTERING;")

# Nur Zeilen mit nicht-null 'absolute_magnitude' behalten und sortieren
filtered_rows = [row for row in rows if row.absolute_magnitude is not None]
sorted_rows = sorted(filtered_rows, key=lambda r: r.absolute_magnitude)[:10]

for row in sorted_rows:
    print(row)

end_time = time.time()
print(f"Abfragezeit: {end_time - start_time:.4f} Sekunden")

# Alle Datensätze abfragen - ALLOW FILTERING ist hier leider notwendig
rows = session.execute("SELECT id, name, absolute_magnitude FROM asteroids ALLOW FILTERING;")

# Filter in Python anwenden
filtered_rows = [row for row in rows if getattr(row, 'absolute_magnitude', None) is not None]
sorted_rows = sorted(filtered_rows, key=lambda r: r.absolute_magnitude)[:10]

for row in sorted_rows:
    print(row)

end_time = time.time()
print(f"Abfragezeit: {end_time - start_time:.4f} Sekunden")


# 5. Asteroid mit einer bestimmten ID abfragen
asteroid_id = 3102762
start_time = time.time()
print(f"\nAsteroid mit ID {asteroid_id}:")
rows = session.execute(f"""
    SELECT * FROM asteroids 
    WHERE id = {asteroid_id};
""")
for row in rows:
    print(row)
end_time = time.time()
print(f"Abfragezeit: {end_time - start_time:.4f} Sekunden")

# Verbindung schließen
cluster.shutdown()
