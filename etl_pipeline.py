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
        print("Loading AI Model (MiniLM)...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')

    def close(self):
        self.driver.close()

    def clear_database(self):
        print("🧹 Membersihkan database lama...")
        with self.driver.session() as session:
            # Hapus semua node dan relationship biar bersih total
            session.run("MATCH (n) DETACH DELETE n")

    def import_base_info(self, csv_file):
        print(f"📂 Mengimport Base Info dari {csv_file}...")
        # CSV: artist,born-died,period,school,url,base,nationality
        
        query = """
        UNWIND $batch AS row
        MERGE (a:Artist {original_name: trim(row.artist)})
        SET a.period = row.period,
            a.school = row.school,
            a.nationality = row.nationality,
            a.source_url = row.url
        """
        self._run_batch_query(query, csv_file)

    def enrich_vip_artists(self, csv_file):
        print(f"✨ Memperkaya data VIP Artists dari {csv_file}...")
        # CSV: id,name,years,genre,nationality,bio,wikipedia,paintings
        
        # NOTE: Kita TIDAK simpan ID angka untuk Artist, biar konsisten pakai Nama saja.
        query = """
        UNWIND $batch AS row
        MATCH (a:Artist {original_name: trim(row.name)})
        SET a.bio = row.bio,
            a.years = row.years,
            a.genre = row.genre,
            a.wikipedia = row.wikipedia
        """
        self._run_batch_query(query, csv_file)

    def import_artworks(self, csv_file):
        print(f"🖼️ Mengimport Artworks dari {csv_file}...")
        
        data = []
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                data = list(reader)
        except FileNotFoundError:
            print(f"❌ Error: File {csv_file} tidak ditemukan.")
            return

        total_rows = len(data)
        batch_size = 500  
        start_time = time.time()

        # CSV Header: ID,artist,title,picture data,file info,jpg url
        # FIX LOGIC:
        # 1. ID Artwork dipaksa jadi Integer (toInteger).
        # 2. Artist dicari berdasarkan nama (trim spasi).
        # 3. Image URL diambil dari row['jpg url'].
        
        query = """
        UNWIND $batch AS row
        
        MERGE (art:Artwork {id: toInteger(row.ID)})
        SET art.title = row.title,
            art.image_url = row.`jpg url`,
            art.meta_data = row.`picture data`,
            art.file_info = row.`file info`
        
        WITH art, row
        MATCH (a:Artist {original_name: trim(row.artist)})
        MERGE (art)-[:CREATED_BY]->(a)
        """

        print(f"   🚀 Memulai import {total_rows} baris data...")

        with self.driver.session() as session:
            for i in range(0, total_rows, batch_size):
                batch = data[i : i + batch_size]
                
                session.run(query, batch=batch)
                
                processed = min(i + batch_size, total_rows)
                elapsed_time = time.time() - start_time
                
                if processed > 0:
                    avg_time_per_row = elapsed_time / processed
                    remaining_rows = total_rows - processed
                    est_remaining_time = remaining_rows * avg_time_per_row
                else:
                    est_remaining_time = 0

                print(f"   ⏳ Progress: {processed}/{total_rows} "
                      f"({(processed/total_rows)*100:.1f}%) | "
                      f"Jalan: {elapsed_time:.0f}s | "
                      f"Sisa: {est_remaining_time:.0f}s", end='\r')

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
        pipeline.clear_database() # WAJIB: Biar ID string yang lama hilang
        pipeline.import_base_info("info_dataset.csv") 
        pipeline.enrich_vip_artists("artists.csv")
        pipeline.import_artworks("artwork_dataset.csv")
    except Exception as e:
        print(f"\n❌ Terjadi Error: {e}")
    finally:
        pipeline.close()