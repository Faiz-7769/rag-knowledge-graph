import spacy
from neo4j import GraphDatabase
from src.knowledge_graph.utils.common import read_json, write_json
from src.knowledge_graph.logger.logging import logger
import re
import itertools
import os
from dotenv import load_dotenv
load_dotenv()

class DataTransformation:

    def __init__(self, config):
        self.config = config
        self.docs = read_json(config.input_json)
        self.nlp = spacy.load("en_core_web_sm")

        # Global storage
        self.entities = []       # List of all entity objects
        self.relationships = []  # Detailed relationship data
        self.triples = []        # Simplified Subject-Verb-Object for Graph
        
        # Counters & Maps
        self.entity_map = {}     # Deduplication map: "Elon Musk_PERSON" -> ID

    def clean_text(self, text):
        """Standardize text: 'Elon Musk ' -> 'elon musk'"""
        return text.strip().lower().replace('"', '').replace("'", "")

    def clean_relation(self, text):
        """Standardize verbs: 'is working for' -> 'WORKING_FOR'"""
        # Remove special chars, replace spaces with _, uppercase
        clean = re.sub(r'[^a-zA-Z0-9\s]', '', text)
        clean = clean.strip().replace(" ", "_").upper()
        return clean if clean else "RELATED_TO"

    # 1️⃣ ENTITY EXTRACTION
    def extract_entities(self):
        logger.info("1. Extracting Entities...")
        
        for doc in self.docs:
            spacy_doc = self.nlp(doc["text"])
            
            for ent in spacy_doc.ents:
                # Create a unique key (Text + Label) to prevent duplicates
                clean_key = f"{self.clean_text(ent.text)}_{ent.label_}"
                
                # We store the "First observed" version of the entity
                if clean_key not in self.entity_map:
                    entity_obj = {
                        "id": clean_key,           # ID for Neo4j
                        "name": ent.text.strip(),  # Display Name
                        "label": ent.label_,       # Type (PERSON, ORG)
                        "doc_id": doc.get("id")
                    }
                    self.entity_map[clean_key] = entity_obj
                    self.entities.append(entity_obj)

        write_json(self.config.entities_output, self.entities)
        logger.info(f"Extracted {len(self.entities)} unique entities.")

    # 2️⃣ RELATIONSHIP EXTRACTION (Co-occurrence Strategy)
    def extract_relationships(self):
        logger.info("2. Extracting Relationships...")
        
        seen_relationships = set() # To avoid duplicate A->B relations in the same sentence

        for doc in self.docs:
            spacy_doc = self.nlp(doc["text"])
            
            for sent in spacy_doc.sents:
                # A. Find all entities in this specific sentence
                sent_entities = []
                for ent in sent.ents:
                    key = f"{self.clean_text(ent.text)}_{ent.label_}"
                    if key in self.entity_map:
                        sent_entities.append(self.entity_map[key])
                
                # We need at least 2 entities to make a relationship
                if len(sent_entities) < 2:
                    continue
                
                # B. Find the "Root Verb" of the sentence (The main action)
                root_verb = "RELATED_TO" # Default fallback
                for token in sent:
                    if token.pos_ == "VERB":
                        root_verb = token.lemma_
                        break # Take the first main verb found
                
                clean_verb = self.clean_relation(root_verb)

                # C. Create Permutations (Connect every entity to every other entity)
                # Note: This is "greedy" but ensures we don't miss connections.
                # Combinations(2) creates pairs: (A,B), (A,C), (B,C)
                for source, target in itertools.combinations(sent_entities, 2):
                    
                    # Prevent self-loops (A->A)
                    if source["id"] == target["id"]:
                        continue

                    # Create a unique signature for this specific link
                    rel_signature = f"{source['id']}|{clean_verb}|{target['id']}"
                    
                    if rel_signature not in seen_relationships:
                        seen_relationships.add(rel_signature)
                        
                        self.relationships.append({
                            "relation_id": rel_signature,
                            "subject_id": source["id"],
                            "subject_name": source["name"],
                            "relation": clean_verb,
                            "object_id": target["id"],
                            "object_name": target["name"],
                            "sentence": sent.text.strip(),
                            "doc_id": doc.get("id")
                        })

        write_json(self.config.relationships_output, self.relationships)
        logger.info(f"Extracted {len(self.relationships)} relationships.")

    # 3️⃣ TRIPLE CREATION
    def create_triples(self):
        logger.info("3. Creating Triples...")
        
        # Convert detailed relationships into simple triples for Graph
        for rel in self.relationships:
            self.triples.append({
                "head_id": rel["subject_id"],
                "head_name": rel["subject_name"],
                "relation": rel["relation"],
                "tail_id": rel["object_id"],
                "tail_name": rel["object_name"]
            })
            
        write_json(self.config.triples_output, self.triples)
        logger.info(f"Generated {len(self.triples)} triples ready for Neo4j.")

    # 4️⃣ GRAPH CONSTRUCTION (Optimized Batch)
    def build_graph(self):
        logger.info("4. Building Graph in Neo4j...")
        neo = self.config
        driver = GraphDatabase.driver(
            os.getenv("NEO_4J_URI"), 
            auth=(neo.neo4j_username, os.getenv("PASSWORD"))
        )

        with driver.session() as session:
            # 1. Unique Constraints
            try:
                session.run("CREATE CONSTRAINT FOR (n:Entity) REQUIRE n.id IS UNIQUE")
            except Exception:
                pass

            # 2. Batch Insert Entities
            logger.info("Batch Inserting Entities...")
            entity_query = """
            UNWIND $batch AS row
            MERGE (e:Entity {id: row.id})
            SET e.name = row.name, 
                e.type = row.label
            """
            self._batch_run(session, entity_query, self.entities)

            # 3. Batch Insert Relationships
            logger.info("Batch Inserting Relationships...")
            
            # Group by Verb to optimize Cypher (e.g., insert all WORKS_AT together)
            triples_by_type = {}
            for t in self.triples:
                r_type = t["relation"]
                if r_type not in triples_by_type: triples_by_type[r_type] = []
                triples_by_type[r_type].append(t)

            for rel_type, batch_data in triples_by_type.items():
                # F-String is safe here because rel_type is sanitized in extract_relationships
                rel_query = f"""
                UNWIND $batch AS row
                MATCH (h:Entity {{id: row.head_id}})
                MATCH (t:Entity {{id: row.tail_id}})
                MERGE (h)-[:{rel_type}]->(t)
                """
                self._batch_run(session, rel_query, batch_data)

        logger.info("Graph Construction Completed Successfully.")

    def _batch_run(self, session, query, data, batch_size=1000):
        """Helper to run queries in chunks"""
        total = len(data)
        for i in range(0, total, batch_size):
            batch = data[i:i+batch_size]
            session.run(query, batch=batch)
