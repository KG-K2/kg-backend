import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

def debug_image_urls():
    driver = GraphDatabase.driver(URI, auth=AUTH)
    with driver.session() as session:
        # Ambil 5 artwork sembarang yang punya image_url
        query = """
        MATCH (a:Artwork) 
        WHERE a.image_url IS NOT NULL 
        RETURN a.id, a.title, a.image_url 
        LIMIT 5
        """
        result = session.run(query)
        
        print("🔍 --- DEBUG URL GAMBAR ---")
        for record in result:
            art_id = record["a.id"]
            title = record["a.title"]
            url = record["a.image_url"]
            
            # Kita print pakai repr() biar kelihatan kalau ada karakter aneh
            print(f"ID: {art_id}")
            print(f"Title: {title}")
            print(f"Raw URL: '{url}'") # Perhatikan tanda petik di output
            
            # Cek Kasus Umum Error
            if " " in url:
                print("   ⚠️  WARNING: Ada spasi di dalam URL!")
            if '"' in url:
                print("   ⚠️  WARNING: Ada tanda kutip ganda di dalam URL!")
            if not url.startswith("http"):
                print("   ⚠️  WARNING: URL tidak diawali http/https!")
                
            print("-" * 30)

    driver.close()

if __name__ == "__main__":
    debug_image_urls()