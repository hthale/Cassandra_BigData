from cassandra.cluster import Cluster

# Verbindung zum Cassandra-Cluster herstellen
cluster = Cluster(['localhost']) 
session = cluster.connect()

# Keyspace-Name definieren
keyspace = 'neo_data' 

# Keyspace löschen
try:
    print(f"Überprüfe, ob der Keyspace '{keyspace}' existiert...")
    rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s", [keyspace])

    if rows.one():
        print(f"Lösche den Keyspace: {keyspace}")
        session.execute(f"DROP KEYSPACE {keyspace};")
        print(f"Keyspace '{keyspace}' wurde erfolgreich gelöscht.")
    else:
        print(f"Keyspace '{keyspace}' existiert nicht.")

except Exception as e:
    print(f"Fehler beim Löschen des Keyspaces: {e}")

finally:
    # Verbindung schließen
    cluster.shutdown()
    print("Verbindung zum Cassandra-Cluster geschlossen.")


    
