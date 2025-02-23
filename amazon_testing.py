import time
import uuid
import pandas as pd
from cassandra.cluster import Cluster

# Verbindung zu einem einzelnen Cassandra-Knoten herstellen
cluster = Cluster(['127.0.0.1'])  # IP-Adresse des Knoten, hier für einen lokalen Knoten
session = cluster.connect()

# Keyspace und Tabelle erstellen
keyspace = 'amazon_products'
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
""")

session.set_keyspace(keyspace)

# Tabelle erstellen (falls nicht vorhanden)
session.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id UUID PRIMARY KEY,
        asin TEXT,
        title TEXT,
        imgUrl TEXT,
        productURL TEXT,
        stars FLOAT,
        reviews INT,
        price FLOAT,
        listPrice FLOAT,
        category_id INT,
        isBestSeller BOOLEAN,
        boughtInLastMonth INT
    );
""")

# Lade die CSV-Datei
csv_file = "amazon_products.csv"  # Passe den Pfad an
df = pd.read_csv(csv_file)

# Funktion zum Importieren der Daten und Messung der Zeit
def import_data():
    query = """
    INSERT INTO products (id, asin, title, imgUrl, productURL, stars, reviews, price, listPrice, category_id, isBestSeller, boughtInLastMonth)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    start_time = time.time()

    # Daten in Cassandra einfügen
    for _, row in df.iterrows():
        session.execute(query, (
            uuid.uuid4(),
            row['asin'],
            row['title'],
            row['imgUrl'],
            row['productURL'],
            row['stars'],
            row['reviews'],
            row['price'],
            row['listPrice'],
            row['category_id'],
            row['isBestSeller'],
            row['boughtInLastMonth']
        ))

    end_time = time.time()
    return end_time - start_time

#  Daten importieren
print(" Importiere Daten...")
time_taken = import_data()
print(f"⏱️ Importdauer für {len(df)} Einträge: {time_taken:.2f} Sekunden")

# Abfrage der ersten 10 Einträge aus der Cassandra-Datenbank
print(" Erste 10 Einträge aus der Cassandra-Datenbank:")
rows = session.execute("SELECT * FROM products LIMIT 10;")
for row in rows:
    print(row)
