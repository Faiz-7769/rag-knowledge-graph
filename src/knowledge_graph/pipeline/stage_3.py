from src.knowledge_graph.config.configuration import ConfigManager
from src.knowledge_graph.components.data_embedding import DataEmbedding
from src.knowledge_graph.exception.exception import KGException
import sys

class DataEmbeddingPipeline:
    def __init__(self):
        pass

    def initiate_data_embedding(self):
        try:
            config = ConfigManager()
            config = config.get_embedding_pipeline_config()
            obj = DataEmbedding(config)
            obj.prepare_chunks()
            vector = obj.generate_embeddings()
            obj.save_vector_store(vector)
            obj.show_faiss_index()
        except Exception as e :
            raise KGException(e,sys)