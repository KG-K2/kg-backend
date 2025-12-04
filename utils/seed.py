import csv
import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

URI = os.getenv("NEO4J_URI")
AUTH = (os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD"))

def seed_data():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    
    # 1. HAPUS DATA LAMA (RESET)
    print("Menghapus data lama...")
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
        # Buat Index Search sekalian biar tidak lupa
        session.run("CREATE FULLTEXT INDEX search_art IF NOT EXISTS FOR (n:Artist|Artwork) ON EACH [n.name, n.title]")

    # 2. UPLOAD ARTISTS
    print("Mengupload Artists...")
    artists = []
    with open('artists.csv', 'r', encoding='utf-8') as f: # Sesuaikan path file csv kamu
        reader = csv.DictReader(f)
        for row in reader:
            artists.append(row)
    
    query_artist = """
    UNWIND $rows AS row
    CREATE (:Artist {
        id: toInteger(row.id),
        name: row.name,
        bio: row.bio,
        nationality: row.nationality,
        genre: row.genre
    })
    """
    with driver.session() as session:
        session.run(query_artist, rows=artists)
    print(f"Sukses upload {len(artists)} Artists.")

    # 3. UPLOAD ARTWORKS (Batching karena data banyak)
    print("Mengupload Artworks (ini agak lama)...")
    batch_size = 1000
    batch = []
    count = 0
    
    query_artwork = """
    UNWIND $rows AS row
    CREATE (:Artwork {
        id: toInteger(row.ID),
        title: row.title,
        artist_name_raw: row.artist,
        url: row['jpg url']
    })
    """
    
    with driver.session() as session:
        with open('artwork_dataset.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                batch.append(row)
                if len(batch) >= batch_size:
                    session.run(query_artwork, rows=batch)
                    count += len(batch)
                    print(f"Uploaded {count} artworks...")
                    batch = []
            # Upload sisa batch terakhir
            if batch:
                session.run(query_artwork, rows=batch)
                print(f"Selesai. Total {count + len(batch)} artworks.")

    # 4. MEMBUAT RELASI (Artist)-[:CREATED]->(Artwork)
    print("Menghubungkan Relasi (Linking)...")
    query_link = """
    MATCH (w:Artwork)
    MATCH (a:Artist)
    WHERE a.name CONTAINS split(w.artist_name_raw, ',')[0]
    MERGE (a)-[:CREATED]->(w)
    """
    with driver.session() as session:
        session.run(query_link)
    
    print("Selesai! Database siap.")
    driver.close()

if __name__ == "__main__":
    seed_data()