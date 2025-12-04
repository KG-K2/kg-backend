import os
import time
from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON
from dotenv import load_dotenv

load_dotenv()

# Setup Neo4j
URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
AUTH = (os.getenv("NEO4J_USER", "neo4j"), os.getenv("NEO4J_PASSWORD", "password"))

# Setup Wikidata
sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
sparql.setReturnFormat(JSON)
sparql.addCustomHttpHeader("User-Agent", "CuratorApp/1.0 (mailto:admin@curator.app)")

class WikidataPipeline:
    def __init__(self):
        self.driver = GraphDatabase.driver(URI, auth=AUTH)

    def close(self):
        self.driver.close()

    # --- FASE 1: LINKING ARTIST (Cari QID) ---
    def get_unlinked_artists(self):
        query = """
        MATCH (a:Artist)
        WHERE a.wikidata_id IS NULL AND a.birth_year IS NOT NULL
        RETURN a.original_name as name, a.birth_year as birth_year
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def find_wikidata_id(self, name, birth_year):
        query = f"""
        SELECT ?item WHERE {{
          ?item wdt:P31 wd:Q5; rdfs:label ?label; wdt:P569 ?dob.
          FILTER(REGEX(?label, "^{name}$", "i"))
          FILTER(YEAR(?dob) >= {birth_year - 1} && YEAR(?dob) <= {birth_year + 1})
        }}
        LIMIT 1
        """
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]
            if bindings:
                return bindings[0]["item"]["value"].split("/")[-1]
        except Exception:
            pass
        return None

    def save_qid(self, name, qid):
        with self.driver.session() as session:
            session.run("MATCH (a:Artist {original_name: $name}) SET a.wikidata_id = $qid", name=name, qid=qid)

    # --- FASE 2: ENRICH ARTIST (Ambil Data Penting Saja) ---
    def get_unenriched_artists(self):
        query = """
        MATCH (a:Artist)
        WHERE a.wikidata_id IS NOT NULL AND a.enriched IS NULL
        RETURN a.original_name as name, a.wikidata_id as qid
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def fetch_artist_data(self, qid):
        # Ambil: Movement, Foto, Lokasi Kerja/Lahir, Guru, Murid
        query = f"""
        SELECT ?movementLabel ?image ?workLocLabel ?birthPlaceLabel ?teacherLabel ?studentLabel WHERE {{
          wd:{qid} wdt:P31 wd:Q5.
          
          OPTIONAL {{ wd:{qid} wdt:P135 ?movement. }}
          OPTIONAL {{ wd:{qid} wdt:P18 ?image. }}
          OPTIONAL {{ wd:{qid} wdt:P937 ?workLoc. }}
          OPTIONAL {{ wd:{qid} wdt:P19 ?birthPlace. }}
          OPTIONAL {{ wd:{qid} wdt:P1066 ?teacher. }}
          OPTIONAL {{ wd:{qid} wdt:P802 ?student. }}
          
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
        """
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            return results["results"]["bindings"]
        except Exception:
            return []

    def save_artist_enrichment(self, name, data):
        with self.driver.session() as session:
            session.run("MATCH (a:Artist {original_name: $name}) SET a.enriched = true", name=name)

            for item in data:
                # 1. FOTO
                if "image" in item:
                    session.run("MATCH (a:Artist {original_name: $name}) SET a.image_url = $url", name=name, url=item["image"]["value"])

                # 2. MOVEMENT (Buat Node Period Baru)
                if "movementLabel" in item:
                    move = item["movementLabel"]["value"]
                    session.run("""
                        MATCH (a:Artist {original_name: $name})
                        MERGE (p:Period {name: $move})
                        MERGE (a)-[:PART_OF_MOVEMENT]->(p)
                    """, name=name, move=move)

                # 3. LOCATION (Buat Node Location Baru)
                loc_name = item.get("workLocLabel", {}).get("value") or item.get("birthPlaceLabel", {}).get("value")
                if loc_name:
                    session.run("""
                        MATCH (a:Artist {original_name: $name})
                        SET a.base_location = $loc
                        MERGE (l:Location {name: $loc})
                        MERGE (a)-[:BASED_IN]->(l)
                    """, name=name, loc=loc_name)
                
                # 4. GURU/MURID (Simpan sebagai relasi teks dulu agar tidak ribet bikin node baru)
                if "teacherLabel" in item:
                    session.run("MATCH (a:Artist {original_name: $name}) SET a.teacher_name = $t", name=name, t=item["teacherLabel"]["value"])
                if "studentLabel" in item:
                    session.run("MATCH (a:Artist {original_name: $name}) SET a.student_name = $s", name=name, s=item["studentLabel"]["value"])

    # --- FASE 3: ENRICH AUXILIARY NODES (Location & Period) ---
    def get_unenriched_aux_nodes(self):
        # Cari Period dan Location yang belum punya gambar/deskripsi
        query = """
        MATCH (n) WHERE (n:Period OR n:Location) AND n.enriched IS NULL
        RETURN labels(n)[0] as type, n.name as name 
        """ 
        # Limit kecil biar gak spam request, nanti diloop terus
        with self.driver.session() as session:
            return session.run(query).data()

    def fetch_aux_data(self, name, node_type):
        # Query lebih umum untuk mencari entity apapun (Tempat atau Aliran)
        query = f"""
        SELECT ?item ?desc ?image ?geo WHERE {{
          ?item rdfs:label "{name}"@en.
          OPTIONAL {{ ?item schema:description ?desc. FILTER(LANG(?desc) = "en") }}
          OPTIONAL {{ ?item wdt:P18 ?image. }}
          OPTIONAL {{ ?item wdt:P625 ?geo. }} 
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        LIMIT 1
        """
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            bindings = results["results"]["bindings"]
            if bindings:
                return bindings[0]
        except Exception:
            return None

    def save_aux_enrichment(self, name, node_type, data):
        query = f"""
        MATCH (n:{node_type} {{name: $name}})
        SET n.enriched = true,
            n.description = $desc,
            n.image_url = $image
        """
        with self.driver.session() as session:
            session.run(query, 
                name=name, 
                desc=data.get("desc", {}).get("value", ""),
                image=data.get("image", {}).get("value", "")
            )

    # --- MAIN RUNNER ---
    def run(self):
        print("🚀 Wikidata Worker (Focused Mode) dimulai...")

        # 3. LOCATION & PERIOD ENRICHMENT
        aux_nodes = self.get_unenriched_aux_nodes()
        print(f"\n🌍 Enriching {len(aux_nodes)} Locations/Periods...")
        for node in aux_nodes:
            print(f"   [{node['type'].upper()}] {node['name']}...")
            data = self.fetch_aux_data(node['name'], node['type'])
            if data:
                self.save_aux_enrichment(node['name'], node['type'], data)
            else:
                # Tandai enriched meski null biar gak dicari lagi
                with self.driver.session() as session:
                    session.run(f"MATCH (n:{node['type']} {{name: $name}}) SET n.enriched = true", name=node['name'])
            time.sleep(0.5)

        print("\n🏁 Selesai satu putaran! (Jalankan lagi nanti jika masih ada sisa)")

if __name__ == "__main__":
    pipeline = WikidataPipeline()
    try:
        pipeline.run()
    except KeyboardInterrupt:
        print("\n⚠️ Stopped.")
    finally:
        pipeline.close()