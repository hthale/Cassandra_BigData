from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

# Verbindung zum Cassandra-Cluster herstellen
cluster = Cluster(['localhost'])  # oder IP-Adresse(n) deines Clusters
session = cluster.connect()
session.set_keyspace('neo_data')

# Neue Asteroiden-Daten
new_asteroids = [
    (101, 'Asteroid Eins', 0.5, 1.0, 12345.6, 98765.4, 'Earth', False, 21.3, True),
    (102, 'Asteroid Zwei', 0.7, 1.2, 11345.6, 97765.4, 'Earth', False, 20.5, False),
    (103, 'Asteroid Drei', 1.0, 2.0, 10345.6, 96765.4, 'Earth', True, 19.8, True),
    (104, 'Asteroid Vier', 0.2, 0.8, 9335.6, 95765.4, 'Earth', False, 22.1, False),
    (105, 'Asteroid Fünf', 0.4, 0.9, 8535.6, 94765.4, 'Earth', False, 21.0, False)
]

# Asteroiden einfügen
insert_query = """
    INSERT INTO asteroids (id, name, est_diameter_min, est_diameter_max, relative_velocity,
    miss_distance, orbiting_body, sentry_object, absolute_magnitude, hazardous)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
prepared_insert = session.prepare(insert_query)

print("Füge 5 neue Asteroiden ein...")
for asteroid in new_asteroids:
    session.execute(prepared_insert, asteroid)

print("Neue Asteroiden erfolgreich eingefügt!")

# Asteroiden über ID löschen
asteroids_to_delete = [101, 102, 103]

delete_query = "DELETE FROM asteroids WHERE id = ?;"
prepared_delete = session.prepare(delete_query)

print("Lösche 3 Asteroiden...")
for asteroid_id in asteroids_to_delete:
    session.execute(prepared_delete, [asteroid_id])

print("Asteroiden erfolgreich gelöscht!")

# Verbindung schließen
cluster.shutdown()

