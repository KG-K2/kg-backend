from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
username = os.getenv("NEO4J_USERNAME", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(uri, auth=(username, password))

def is_read_only(query: str):
    write_keywords = ["CREATE", "MERGE", "SET", "DELETE", "INSERT", "CALL"]

    if any(keyword in query.upper() for keyword in write_keywords):
        return False
    return True


def run_custom_query(query: str):
    if not(is_read_only(query)):
        return {"error": "query should be read-only."}
    
    try:
        with driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]
    except Exception as e:
        return {"error": str(e)}

def search_graph(search_term: str):
    fuzzy_term = f"{search_term}~"
    
    # Update Query Search: Ambil detail lengkap untuk kartu hasil search
    cypher_query = """
    CALL db.index.fulltext.queryNodes("search_art", $term) YIELD node, score
    WITH node, score, labels(node)[0] as type,
        toLower(COALESCE(node.name, node.title, node.original_name)) AS label_lower

    // --- Score Boost: If exact token match inside title/label, add +5 ---
    WITH node, type, score,
        (score +
            CASE 
                WHEN label_lower CONTAINS toLower($raw) THEN 5
                ELSE 0
            END
        ) AS boosted_score

    OPTIONAL MATCH (node)-[:CREATED_BY]->(a:Artist)

    RETURN 
        CASE WHEN 'Artist' IN labels(node) THEN node.original_name ELSE node.id END as id,
        type,
        COALESCE(node.name, node.title, node.original_name) as label,
        boosted_score AS score,
        CASE 
            WHEN 'Artwork' IN labels(node) THEN {
                url: node.image_url,
                title: node.title,
                artist_name_raw: a.original_name,
                year: node.year_created,    
                medium: node.medium
            }
            ELSE {
                bio: node.bio,
                nationality: node.nationality,
                years: toString(node.birth_year) + ' - ' + toString(node.death_year)
            }
        END as details
    ORDER BY score DESC
    LIMIT 50
    """
    
    try:
        with driver.session() as session:
            result = session.run(cypher_query, term=fuzzy_term, raw=search_term.lower())
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
    # UPDATE: Tambahkan bagian Vector Search untuk rekomendasi
    query = """
    MATCH (a:Artwork {id: $art_id})
    
    // 1. AMBIL DETAIL UTAMA (Sama kayak sebelumnya)
    OPTIONAL MATCH (a)-[:CREATED_BY]->(artist:Artist)
    
    // 2. FITUR AI: CARI YANG MIRIP (Nearest Neighbor)
    // Menggunakan index 'art_embeddings_index' yang sudah kita buat
    // Kita cari 6, nanti yang ke-1 pasti dirinya sendiri (skor 1.0), jadi kita skip
    CALL db.index.vector.queryNodes('art_embeddings_index', 6, a.embedding)
    YIELD node as similar, score
    WHERE similar.id <> a.id  // Pastikan bukan lukisan itu sendiri
    
    // Kumpulkan 5 rekomendasi terbaik
    WITH a, artist, collect({
        id: similar.id,
        title: similar.title,
        url: similar.image_url,
        score: score
    })[..5] as similar_artworks
    
    RETURN a.id AS id, 
           a.title AS title, 
           a.image_url AS image_url,
           a.medium AS medium,              
           a.dimensions AS dimensions,      
           a.year_created AS year,          
           a.location AS location,
           a.raw_metadata AS raw_meta,      
           
           artist.original_name AS artist_name,
           artist.nationality AS artist_nation,
           artist.base_location AS artist_base,
           artist.bio AS artist_bio,
           artist.birth_year AS b_year,
           artist.death_year AS d_year,
           artist.period AS period,
           artist.school AS school,
           
           similar_artworks  // <--- RETURN BARU
    """
    result = tx.run(query, art_id=int(art_id)).single()
    
    if not result:
        return None
        
    return {
        "artwork": {
            "id": result["id"],
            "title": result["title"],
            "url": result["image_url"],
            "year": result["year"] or "Unknown Year",
            "medium": result["medium"] or "Unknown Medium",
            "dimensions": result["dimensions"] or "Unknown Dimensions",
            "location": result["location"] or "Unknown Location",
            "description": result["raw_meta"],
            "type": "Artwork"
        },
        "artist": {
            "id": result["artist_name"],
            "name": result["artist_name"],
            "nationality": result["artist_nation"],
            "base": result["artist_base"],
            "bio": result["artist_bio"],
            "birth_year": result["b_year"],
            "death_year": result["d_year"],
            "period": result["period"],
            "school": result["school"],
            "type": "Artist"
        } if result["artist_name"] else None,
        "similar": result["similar_artworks"] # Masukkan ke response JSON
    }

def get_artist_by_name(tx, artist_name):
    query = """
    MATCH (a:Artist {original_name: $name})
    OPTIONAL MATCH (w:Artwork)-[:CREATED_BY]->(a)
    RETURN a.original_name AS name, 
           a.bio AS bio, 
           a.nationality AS nationality,
           a.base_location AS base,      // <--- Ambil ini
           a.birth_year AS b_year,
           a.death_year AS d_year,
           a.period AS period,
           a.school AS school,
           
           collect({
               id: w.id,
               title: w.title,
               url: w.image_url,
               year: w.year_created,
               medium: w.medium
           }) AS artworks
    """
    result = tx.run(query, name=artist_name.strip()).single()
    
    if not result:
        return None

    valid_artworks = [art for art in result["artworks"] if art["id"] is not None]

    return {
        "id": result["name"],
        "name": result["name"],
        "bio": result["bio"],
        "nationality": result["nationality"],
        "base": result["base"],         # <--- Masukkan ke return dict
        "birth_year": result["b_year"],
        "death_year": result["d_year"],
        "period": result["period"],
        "school": result["school"],
        "type": "Artist",
        "artworks": valid_artworks
    }

def get_location_details(tx, name):
    # Ambil detail Location + Top Artists + Top Artworks
    query = """
    MATCH (l:Location {name: $name})
    
    // 1. Ambil Artists yang based di sini
    OPTIONAL MATCH (a:Artist)-[:BASED_IN]->(l)
    WITH l, collect(DISTINCT {name: a.original_name, role: 'Resident'})[..12] as artists
    
    // 2. Ambil Artworks yang terkait lokasi ini (via string match atau relasi kalau ada)
    OPTIONAL MATCH (art:Artwork) WHERE art.location CONTAINS $name
    WITH l, artists, collect(DISTINCT {id: art.id, title: art.title, url: art.image_url})[..8] as artworks
    
    RETURN l.name as name,
           l.description as description,
           l.image_url as image,
           artists,
           artworks
    """
    result = tx.run(query, name=name).single()
    return result.data() if result else None

def get_movement_details(tx, name):
    # Ambil detail Period/Movement + Top Artists + Top Artworks
    query = """
    MATCH (p:Period {name: $name})
    
    // 1. Ambil Artists di movement ini
    OPTIONAL MATCH (a:Artist)-[:PART_OF_MOVEMENT]->(p)
    WITH p, collect(DISTINCT {name: a.original_name})[..12] as artists
    
    // 2. Ambil Artworks dari artist di movement ini
    OPTIONAL MATCH (a)-[:PART_OF_MOVEMENT]->(p)
    MATCH (art:Artwork)-[:CREATED_BY]->(a)
    WITH p, artists, collect(DISTINCT {id: art.id, title: art.title, url: art.image_url})[..8] as artworks
    
    RETURN p.name as name,
           p.description as description,
           p.image_url as image,
           artists,
           artworks
    """
    result = tx.run(query, name=name).single()
    return result.data() if result else None

def get_year_details(tx, year_value):
    query = """
    MATCH (y:Year {value: $year})
    
    // 1. Siapa yang LAHIR tahun ini?
    OPTIONAL MATCH (born:Artist)-[:BORN_IN]->(y)
    WITH y, collect(DISTINCT {name: born.original_name, role: 'Born'}) as born_list
    
    // 2. Siapa yang MENINGGAL tahun ini?
    OPTIONAL MATCH (died:Artist)-[:DIED_IN]->(y)
    WITH y, born_list, collect(DISTINCT {name: died.original_name, role: 'Died'}) as died_list
    
    // 3. Karya apa yang DIBUAT tahun ini?
    OPTIONAL MATCH (art:Artwork)-[:CREATED_IN]->(y)
    WITH y, born_list, died_list, collect(DISTINCT {id: art.id, title: art.title, url: art.image_url})[..12] as artworks
    
    RETURN y.value as year,
           born_list,
           died_list,
           artworks
    """
    try:
        # Pastikan year di-cast ke integer
        result = tx.run(query, year=int(year_value)).single()
        return result.data() if result else None
    except ValueError:
        return None


