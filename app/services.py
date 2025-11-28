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