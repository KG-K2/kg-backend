from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(uri, auth=(username, password))

def run_custom_query(query: str):
    try:
        with driver.session() as session:
            result = session.run(query)
            records = [record.data() for record in result]
            return records
    except Exception as e:
        return {"error": str(e)}
    

def search_graph(search_term: str):
    """
    Melakukan pencarian fulltext fuzzy pada Artist dan Artwork.
    """
    # Menambahkan tilde (~) di akhir kata untuk fuzzy matching (toleransi typo)
    # Contoh: "Monet" -> "Monet~"
    fuzzy_term = f"{search_term}~"
    
    cypher_query = """
    CALL db.index.fulltext.queryNodes("search_art", $term) YIELD node, score
    RETURN 
        id(node) as id,
        labels(node)[0] as type,
        COALESCE(node.name, node.title) as label,
        properties(node) as details,
        score
    ORDER BY score DESC
    LIMIT 20
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher_query, term=fuzzy_term)
            # Format hasil agar sesuai dengan pydantic model
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
    
def get_artist_by_id(node_id: int):
    """
    Mengambil data Artist beserta semua Artwork buatannya.
    """
    # PERBAIKAN 1: Ganti 'WHERE id(a)' menjadi 'WHERE a.id'
    query = """
    MATCH (a:Artist) 
    WHERE a.id = $id
    OPTIONAL MATCH (a)-[:CREATED]->(w:Artwork)
    RETURN a, collect(w) as artworks
    """
    try:
        with driver.session() as session:
            result = session.run(query, id=node_id).single()
            if not result:
                return None
            
            artist_node = result["a"]
            artworks_nodes = result["artworks"]
            
            # PERBAIKAN 2: Ambil ID dari properti CSV (.get("id")), bukan internal ID
            artist_data = {
                "id": artist_node.get("id"), 
                "name": artist_node.get("name"),
                "bio": artist_node.get("bio"),
                "nationality": artist_node.get("nationality"),
                "type": "Artist",
                "artworks": []
            }

            for w in artworks_nodes:
                artist_data["artworks"].append({
                    "id": w.get("id"),  # <--- Pastikan ini juga ambil properti id
                    "title": w.get("title"),
                    "url": w.get("url"),
                    "form": w.get("form", "Unknown"),
                    "type": "Artwork"
                })
                
            return artist_data
    except Exception as e:
        print(f"Error fetching artist: {e}")
        return None

def get_artwork_by_id(node_id: int):
    """
    Mengambil data Artwork beserta Artist pembuatnya.
    """
    # PERBAIKAN 1: Ganti 'WHERE id(w)' menjadi 'WHERE w.id'
    query = """
    MATCH (w:Artwork) 
    WHERE w.id = $id
    OPTIONAL MATCH (a:Artist)-[:CREATED]->(w)
    RETURN w, a
    """
    try:
        with driver.session() as session:
            result = session.run(query, id=node_id).single()
            if not result:
                return None
            
            w = result["w"]
            a = result["a"]
            
            # PERBAIKAN 2: Ambil ID dari properti CSV
            artwork_data = {
                "id": w.get("id"), 
                "title": w.get("title"),
                "url": w.get("url"),
                "location": w.get("location", "Unknown Location"),
                "form": w.get("form", "Painting"), 
                "type": "Artwork"
            }
            
            artist_data = None
            if a:
                artist_data = {
                    "id": a.get("id"), # <--- Ambil properti id
                    "name": a.get("name"),
                    "nationality": a.get("nationality"),
                    "bio": a.get("bio"),
                    "type": "Artist"
                }
                
            return {"artwork": artwork_data, "artist": artist_data}
            
    except Exception as e:
        print(f"Error fetching artwork: {e}")
        return None