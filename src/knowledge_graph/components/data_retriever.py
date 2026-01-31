import sys
from langchain_core.retrievers import BaseRetriever
from langchain_core.callbacks import CallbackManagerForRetrieverRun
from langchain_core.documents import Document
from src.knowledge_graph.exception.exception import KGException
from src.knowledge_graph.logger.logging import logger
from typing import List, Any
from dotenv import load_dotenv
load_dotenv()
class HybridRetriever(BaseRetriever):
    """
    Same Retriever logic as before (Vectors + Graph)
    """
    vector_index: Any
    vector_metadata: List[dict]
    embedder: Any
    graph: Any
    nlp: Any
    top_k_vector: int = 5
    top_k_graph: int = 5

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        logger.info("Initializing HybridRetriever")

    def _get_relevant_documents(
        self, query: str, *, run_manager: CallbackManagerForRetrieverRun
    ) -> List[Document]:
        logger.info("Initializing vector search")
        # 1. Vector Search
        query_vector = self.embedder.encode([query])
        _, indices = self.vector_index.search(query_vector, self.top_k_vector)
        
        docs = []
        for idx in indices[0]:
            if idx < len(self.vector_metadata):
                meta = self.vector_metadata[idx]
                content = f"[Source: {meta.get('source_name', 'Unknown')}] {meta.get('text', '')}"
                docs.append(Document(
                    page_content=content,
                    metadata={"type": "vector", "source": meta.get('source_name')}
                ))
        logger.info("Vector search completed, proceeding to graph search")
        # 2. Graph Search (Fixed for neo4j.Driver)
        spacy_doc = self.nlp(query)
        entities = [ent.text for ent in spacy_doc.ents]
        logger.info("Opening Graph session for entity search")
        if entities:
            # Open a session properly using the driver
            try:
                with self.graph.session() as session:
                    for entity in entities:
                        # Fuzzy match entity names
                        cypher = """
                        MATCH (n:Entity)-[r]-(m:Entity)
                        WHERE toLower(n.name) CONTAINS toLower($name)
                        RETURN n.name, type(r) AS rel, m.name
                        LIMIT $limit
                        """
                        # Use session.run with parameters (safer than f-strings)
                        result = session.run(cypher, name=entity, limit=self.top_k_graph)
                        
                        for record in result:
                            fact = f"{record['n.name']} --[{record['rel']}]--> {record['m.name']}"
                            docs.append(Document(
                                page_content=fact,
                                metadata={"type": "graph", "entity": entity}
                            ))
                logger.info("Graph search completed")
            except Exception as e:
                raise KGException(e,sys)
        
        return docs