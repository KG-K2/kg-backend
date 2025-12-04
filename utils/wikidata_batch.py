import os
import time
from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
from dotenv import load_dotenv

load_dotenv()

URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(JSON)
sparql.addCustomHttpHeader("User-Agent", "CuratorApp/BatchWorker/1.0 (mailto:admin@curator.app)")

class WikidataBatchPipeline:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)

    def close(self):
        self.driver.close()

    def get_unlinked_artists_batch(self, batch_size=50):
        # Ambil 50 artist sekaligus yang belum punya QID
        query = f"""
        MATCH (a:Artist)
        WHERE a.wikidata_id IS NULL AND a.birth_year IS NOT NULL
        RETURN a.original_name as name, a.birth_year as year
        LIMIT {batch_size}
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def fetch_batch_qids(self, artists):
        """
        Kirim 50 nama sekaligus ke Wikidata pakai klausa VALUES.
        Ini teknik rahasia biar ngebut!
        """
        if not artists: return {}

        # 1. Susun string values: ("Picasso" 1881) ("Dali" 1904) ...
        values_str = ""
        for art in artists:
            # Escape tanda kutip kalau ada di nama
            safe_name = art['name'].replace('"', '\\"')
            values_str += f'("{safe_name}" {art["year"]}) '

        # 2. Query SPARQL Batch
        query = f"""
        SELECT ?inputName ?item WHERE {{
          VALUES (?inputName ?inputYear) {{ {values_str} }}
          
          ?item wdt:P31 wd:Q5;          # Manusia
                rdfs:label ?label;      # Punya Label
                wdt:P569 ?dob.          # Punya Tgl Lahir
          
          # Cocokkan Label (Case Insensitive)
          FILTER(LCASE(STR(?label)) = LCASE(?inputName))
          
          # Cocokkan Tahun (+/- 1 tahun toleransi)
          FILTER(YEAR(?dob) >= ?inputYear - 1 && YEAR(?dob) <= ?inputYear + 1)
        }}
        """
        
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            
            # Mapping hasil: { "Picasso": "Q5593", ... }
            found_map = {}
            for row in results["results"]["bindings"]:
                name = row["inputName"]["value"]
                qid = row["item"]["value"].split("/")[-1]
                found_map[name] = qid
            
            return found_map

        except Exception as e:
            print(f"❌ Batch Error: {e}")
            return {}

    def save_batch_qids(self, qid_map):
        if not qid_map: return
        
        # Update Neo4j sekaligus (pakai UNWIND biar 1 transaksi)
        params = [{"name": k, "qid": v} for k, v in qid_map.items()]
        
        query = """
        UNWIND $batch as row
        MATCH (a:Artist {original_name: row.name})
        SET a.wikidata_id = row.qid
        """
        with self.driver.session() as session:
            session.run(query, batch=params)

    # --- ENRICHMENT BATCH (Ambil Foto & Desc Sekaligus) ---
    
    def get_unenriched_qids(self, limit=50):
        query = f"""
        MATCH (a:Artist)
        WHERE a.wikidata_id IS NOT NULL AND a.enriched IS NULL
        RETURN a.original_name as name, a.wikidata_id as qid
        LIMIT {limit}
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def fetch_batch_details(self, artist_list):
        if not artist_list: return {}
        
        # Susun QID: wd:Q5593 wd:Q1234 ...
        qids_str = " ".join([f"wd:{a['qid']}" for a in artist_list])
        
        query = f"""
        SELECT ?item ?image ?desc ?movementLabel WHERE {{
          VALUES ?item {{ {qids_str} }}
          
          OPTIONAL {{ ?item wdt:P18 ?image. }}
          OPTIONAL {{ ?item schema:description ?desc. FILTER(LANG(?desc) = "en") }}
          OPTIONAL {{ ?item wdt:P135 ?movement. }}
          
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            
            # Grouping results by QID
            data_map = {}
            for row in results["results"]["bindings"]:
                qid = row["item"]["value"].split("/")[-1]
                if qid not in data_map: data_map[qid] = {}
                
                if "image" in row: data_map[qid]["image"] = row["image"]["value"]
                if "desc" in row: data_map[qid]["desc"] = row["desc"]["value"]
                if "movementLabel" in row: data_map[qid]["movement"] = row["movementLabel"]["value"]
                
            return data_map
        except Exception as e:
            print(f"❌ Detail Error: {e}")
            return {}

    def save_batch_details(self, data_map):
        if not data_map: return

        # Kita update satu-satu di loop lokal karena logic-nya agak kompleks (conditional update)
        with self.driver.session() as session:
            for qid, info in data_map.items():
                # Tandai enriched
                session.run("MATCH (a:Artist {wikidata_id: $qid}) SET a.enriched = true", qid=qid)
                
                if "image" in info:
                    session.run("MATCH (a:Artist {wikidata_id: $qid}) SET a.image_url = $url", qid=qid, url=info["image"])
                
                if "desc" in info:
                    session.run("""
                        MATCH (a:Artist {wikidata_id: $qid}) 
                        WHERE a.bio IS NULL OR a.bio = 'No biography available.' 
                        SET a.bio = $d
                    """, qid=qid, d=info["desc"])
                    
                if "movement" in info:
                    session.run("""
                        MATCH (a:Artist {wikidata_id: $qid})
                        MERGE (p:Period {name: $mov})
                        MERGE (a)-[:PART_OF_MOVEMENT]->(p)
                    """, qid=qid, mov=info["movement"])

    def run(self):
        print("🏎️  Wikidata BATCH Worker dimulai...")
        
        while True:
            # 1. LINKING BATCH
            unlinked = self.get_unlinked_artists_batch(batch_size=50)
            if unlinked:
                print(f"🔗 Processing batch of {len(unlinked)} artists for Linking...")
                found_map = self.fetch_batch_qids(unlinked)
                if found_map:
                    self.save_batch_qids(found_map)
                    print(f"   ✅ Matched {len(found_map)}/{len(unlinked)} IDs.")
                else:
                    print("   ⚠️  No matches in this batch.")
            
            # 2. ENRICHMENT BATCH
            unenriched = self.get_unenriched_qids(limit=50)
            if unenriched:
                print(f"✨ Processing batch of {len(unenriched)} artists for Enrichment...")
                details_map = self.fetch_batch_details(unenriched)
                self.save_batch_details(details_map)
                print(f"   ✅ Enriched {len(details_map)} artists.")

            if not unlinked and not unenriched:
                print("🏁 Semua data sudah diproses! Tidur dulu...")
                break
            
            print("💤 Cooldown 2 detik biar server Wikidata gak marah...")
            time.sleep(2)

if __name__ == "__main__":
    pipeline = WikidataBatchPipeline()
    pipeline.run()