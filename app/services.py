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
    