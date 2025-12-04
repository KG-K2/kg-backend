from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(uri, auth=(username, password))

def run_custom_query(query: str):
    try:
        with driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]
    except Exception as e:
        return {"error": str(e)}

def search_graph(search_term: str):
    fuzzy_term = f"{search_term}~"
    
    # Query ini diperbaiki untuk:
    # 1. Mengambil nama Artist jika hasil search adalah Artwork (OPTIONAL MATCH)
    # 2. Mengubah 'image_url' menjadi 'url' agar Frontend bisa baca
    # 3. Mengembalikan ID: Angka untuk Artwork, Nama untuk Artist
    
    cypher_query = """
    CALL db.index.fulltext.queryNodes("search_art", $term) YIELD node, score
    
    // Cek tipe node
    WITH node, score, labels(node)[0] as type
    
    // Jika Artwork, cari siapa Artist-nya
    OPTIONAL MATCH (node)-[:CREATED_BY]->(a:Artist)
    
    RETURN 
        // ID Logic: Kalau Artist pakai nama, kalau Artwork pakai ID angka
        CASE 
            WHEN 'Artist' IN labels(node) THEN node.original_name 
            ELSE node.id 
        END as id,
        
        type,
        
        COALESCE(node.name, node.title, node.original_name) as label,
        
        score,
        
        // Detail Logic: Mapping field database ke kebutuhan Frontend
        CASE 
            WHEN 'Artwork' IN labels(node) THEN {
                url: node.image_url,            // INI KUNCINYA: Frontend minta 'url'
                title: node.title,
                artist_name_raw: a.original_name, // Biar nama artist muncul di kartu artwork
                form: node.meta_data,
                location: node.location
            }
            ELSE {
                bio: node.bio,
                nationality: node.nationality,
                years: node.years
            }
        END as details
    ORDER BY score DESC
    LIMIT 20
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher_query, term=fuzzy_term)
            records = [
                {
                    "id": record["id"],
                    "type": record["type"],
                    "label": record["label"],
                    "score": record["score"],
                    "details": record["details"]
                } 
                for record in result
            ]
            return records
    except Exception as e:
        print(f"Search Error: {e}")
        return []

def get_artwork_by_id(tx, art_id):
    query = """
    MATCH (a:Artwork {id: $art_id})
    OPTIONAL MATCH (a)-[:CREATED_BY]->(artist:Artist)
    RETURN a.id AS id, 
           a.title AS title, 
           a.image_url AS image_url,
           a.meta_data AS meta,
           a.file_info AS form,
           a.location AS location,
           artist.original_name AS artist_name,
           artist.nationality AS artist_nation,
           artist.bio AS artist_bio
    """
    result = tx.run(query, art_id=int(art_id)).single()
    
    if not result:
        return None
        
    return {
        "artwork": {
            "id": result["id"],
            "title": result["title"],
            "url": result["image_url"],
            "form": result["form"] or result["meta"], # Fallback ke metadata
            "location": result["location"] or "Unknown Location",
            "type": "Artwork"
        },
        "artist": {
            "id": result["artist_name"], # ID Artist = Nama
            "name": result["artist_name"],
            "nationality": result["artist_nation"],
            "bio": result["artist_bio"],
            "type": "Artist"
        } if result["artist_name"] else None
    }

def get_artist_by_name(tx, artist_name):
    # Query ini mengambil data Artist DAN semua Artwork buatannya
    query = """
    MATCH (a:Artist {original_name: $name})
    OPTIONAL MATCH (w:Artwork)-[:CREATED_BY]->(a)
    RETURN a.original_name AS name, 
           a.bio AS bio, 
           a.years AS years,
           a.nationality AS nationality,
           collect({
               id: w.id,
               title: w.title,
               url: w.image_url,
               form: w.meta_data
           }) AS artworks
    """
    result = tx.run(query, name=artist_name.strip()).single()
    
    if not result:
        return None

    # Bersihkan artwork yang null (jika artist tidak punya karya)
    valid_artworks = [art for art in result["artworks"] if art["id"] is not None]

    return {
        "id": result["name"], # ID = Nama
        "name": result["name"],
        "bio": result["bio"],
        "nationality": result["nationality"],
        "type": "Artist",
        "artworks": valid_artworks
    }