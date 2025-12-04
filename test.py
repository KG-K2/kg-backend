import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load credential dari .env
load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

def test_graph():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    session = driver.session()

    print("📊 --- HASIL VALIDASI KNOWLEDGE GRAPH ---")

    # 1. Cek Jumlah Node & Relasi
    query_stats = """
    MATCH (a:Artist) WITH count(a) as artist_count
    MATCH (w:Artwork) WITH artist_count, count(w) as artwork_count
    MATCH ()-[r:CREATED_BY]->() WITH artist_count, artwork_count, count(r) as rel_count
    RETURN artist_count, artwork_count, rel_count
    """
    result = session.run(query_stats).single()
    print(f"\n✅ Total Artist   : {result['artist_count']}")
    print(f"✅ Total Artwork  : {result['artwork_count']}")
    print(f"✅ Total Relasi   : {result['rel_count']}")

    # 2. Cek Sampel Data (Hans von Aachen)
    # Kita cek apakah data Hans (dari CSV kamu tadi) masuk lengkap
    print("\n🔍 --- Cek Sampel Data Specific (Hans von Aachen) ---")
    query_sample = """
    MATCH (a:Artist {original_name: 'AACHEN, Hans von'})<-[:CREATED_BY]-(w:Artwork)
    RETURN a.nationality as nation, w.title as title, w.image_url as img
    LIMIT 3
    """
    results = session.run(query_sample).data()
    
    if results:
        print(f"Found {len(results)} artworks for Hans von Aachen:")
        for record in results:
            print(f"   - [{record['nation']}] {record['title']} -> {record['img']}")
    else:
        print("❌ Warning: Hans von Aachen tidak ditemukan atau tidak punya artwork.")

    # 3. Cek Data yang 'Yatim Piatu' (Artwork tanpa Artist)
    # Ini penting untuk tahu kualitas linking data
    query_orphan = """
    MATCH (w:Artwork)
    WHERE NOT (w)-[:CREATED_BY]->(:Artist)
    RETURN count(w) as orphan_count
    """
    orphan = session.run(query_orphan).single()['orphan_count']
    print(f"\n⚠️ Artwork tanpa Artist (Orphan): {orphan}")
    if orphan > 0:
        print("   (Ini wajar jika nama artist di csv artwork beda sedikit dgn csv artist)")

    driver.close()

if __name__ == "__main__":
    test_graph()