import os
import csv
import time
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

class ArtGraphPipeline:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)
        print("🤖 Loading AI Model (MiniLM) untuk Embedding...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def close(self):
        self.driver.close()

    def clear_database(self):
        print("🧹 Membersihkan database lama & Index yang nyangkut...")
        with self.driver.session() as session:
            # 1. Hapus Data
            session.run("MATCH (n) DETACH DELETE n")
            
            # 2. Hapus Constraint & Index secara paksa
            # Kita ambil daftar constraint dulu, lalu drop satu-satu
            constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
            for rec in constraints:
                print(f"   - Dropping constraint: {rec['name']}")
                session.run(f"DROP CONSTRAINT {rec['name']} IF EXISTS")

            indexes = session.run("SHOW INDEXES YIELD name").data()
            for rec in indexes:
                # Jangan hapus index bawaan Neo4j (lookup)
                if rec['name'] != "index_343aff4e": 
                    print(f"   - Dropping index: {rec['name']}")
                    session.run(f"DROP INDEX {rec['name']} IF EXISTS")

    def create_indexes(self):
        print("⚙️ Membuat Index Pencarian & Vector...")
        with self.driver.session() as session:
            # Constraint harus dibuat DULUAN sebelum index lain biar gak konflik
            session.run("CREATE CONSTRAINT artist_uniq IF NOT EXISTS FOR (a:Artist) REQUIRE a.original_name IS UNIQUE")
            session.run("CREATE CONSTRAINT artwork_uniq IF NOT EXISTS FOR (a:Artwork) REQUIRE a.id IS UNIQUE")
            
            # Baru buat Vector Index
            session.run("""
                CREATE VECTOR INDEX art_embeddings_index IF NOT EXISTS
                FOR (n:Artwork) ON (n.embedding)
                OPTIONS {indexConfig: {
                 `vector.dimensions`: 384,
                 `vector.similarity_function`: 'cosine'
                }}
            """)
            
            # Terakhir buat Fulltext Search Index
            # (Pastikan index sebelumnya udah beres, biasanya aman kalau constraint udah jadi)
            session.run("""
                CREATE FULLTEXT INDEX search_art IF NOT EXISTS
                FOR (n:Artwork|Artist) 
                ON EACH [n.title, n.original_name, n.nationality, n.period]
            """)

    def import_base_info(self, csv_file):
        print(f"📂 Mengimport Base Info dari {csv_file}...")
        
        # PENTING: Pakai 'row.clean_name' hasil normalisasi
        query = """
        UNWIND $batch AS row
        MERGE (a:Artist {original_name: row.clean_name})
        SET a.period = row.period,
            a.school = row.school,
            a.nationality = row.nationality,
            a.base_location = row.base,
            a.source_url = row.url,
            a.birth_year = toInteger(row.birth_year_clean),
            a.death_year = toInteger(row.death_year_clean)
        """
        self._run_batch_query(query, csv_file)

    def enrich_vip_artists(self, csv_file):
        print(f"✨ Memperkaya VIP Artists dari {csv_file}...")
        
        # PENTING: Pakai 'row.clean_name'
        query = """
        UNWIND $batch AS row
        MATCH (a:Artist {original_name: row.clean_name})
        SET a.bio = row.bio,
            a.wikipedia = row.wikipedia,
            a.birth_year = coalesce(a.birth_year, toInteger(row.birth_year_clean)),
            a.death_year = coalesce(a.death_year, toInteger(row.death_year_clean))
        """
        self._run_batch_query(query, csv_file)

    def import_artworks(self, csv_file):
        print(f"🖼️ Mengimport Artworks + Embeddings dari {csv_file}...")
        
        data = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
        except FileNotFoundError:
            print(f"❌ Error: File {csv_file} tidak ditemukan.")
            return

        total_rows = len(data)
        batch_size = 1000 
        start_time = time.time()

        # PENTING: Artist di-match pakai 'row.clean_artist_name'
        query = """
        UNWIND $batch AS row
        
        MERGE (art:Artwork {id: toInteger(row.ID)})
        SET art.title = row.title,
            art.image_url = row.clean_url,
            art.file_info = row.`file info`,
            art.year_created = row.clean_year,
            art.medium = row.clean_medium,
            art.dimensions = row.clean_dimensions,
            art.location = row.clean_location,
            art.raw_metadata = row.`picture data`,
            art.embedding = row.embedding 
        
        WITH art, row
        MATCH (a:Artist {original_name: row.clean_artist_name})
        MERGE (art)-[:CREATED_BY]->(a)
        """

        print(f"   🚀 Memulai import {total_rows} artworks...")

        with self.driver.session() as session:
            for i in range(0, total_rows, batch_size):
                batch = data[i : i + batch_size]
                
                # --- EMBEDDING ---
                texts_to_embed = []
                for row in batch:
                    # Pakai nama bersih untuk embedding juga biar akurat
                    desc = f"{row['title']} by {row['clean_artist_name']}. {row['clean_medium']}. {row['clean_year']}."
                    texts_to_embed.append(desc)
                
                embeddings = self.model.encode(texts_to_embed)
                
                for idx, row in enumerate(batch):
                    row['embedding'] = embeddings[idx].tolist()
                # ------------------

                session.run(query, batch=batch)
                
                processed = min(i + batch_size, total_rows)
                elapsed_time = time.time() - start_time
                avg_time = elapsed_time / processed if processed > 0 else 0
                est_remain = (total_rows - processed) * avg_time
                
                print(f"   ⏳ Progress: {processed}/{total_rows} "
                      f"({(processed/total_rows)*100:.1f}%) | "
                      f"Sisa: {est_remain:.0f}s", end='\r')

        total_time = time.time() - start_time
        print(f"\n✅ Selesai import {total_rows} Artworks dalam {total_time:.2f} detik.")

    def _run_batch_query(self, query, csv_file, batch_size=1000):
        data = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
        except FileNotFoundError:
            print(f"❌ File {csv_file} not found.")
            return

        with self.driver.session() as session:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                session.run(query, batch=batch)
        
        print(f"✅ Selesai memproses {len(data)} baris.")

if __name__ == "__main__":
    pipeline = ArtGraphPipeline()
    try:
        pipeline.clear_database() # 1. Hapus constraint lama
        pipeline.create_indexes() # 2. Buat constraint baru yang bersih
        
        pipeline.import_base_info("cleaned_info.csv") 
        pipeline.enrich_vip_artists("cleaned_artists.csv")
        pipeline.import_artworks("cleaned_artworks.csv")
        
    except Exception as e:
        print(f"\n❌ Terjadi Error: {e}")
    finally:
        pipeline.close()