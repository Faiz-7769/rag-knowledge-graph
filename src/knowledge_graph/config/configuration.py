from src.knowledge_graph.utils.common import read_yaml
from src.knowledge_graph.entity.config_entity import (DataIngestionConfig,DataTransformationConfig,
                                                      EmbeddingPipelineConfig,ChunkingConfig,EmbeddingModelConfig,
                                                      VectorStoreConfig,
                                                      faiss_data,llmconfig,neo4j_config,Ragpipelineconfig)
from src.knowledge_graph.constants import *

class ConfigManager:
    def __init__(self, config_path=CONFIG_FILE_PATH):
        self.config = read_yaml(config_path)

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        config = self.config.data_ingestion
        return DataIngestionConfig(
            root_dir=config.root_dir,
            email_dir=config.email_dir,
            pdf_dir=config.pdf_dir,
            csv_dir=config.csv_dir,
            db_path=config.db_path,
            output_json=config.output_json,
        )
    def get_data_transformation_config(self) -> DataTransformationConfig:
        config = self.config.data_transformation

        return DataTransformationConfig(
            input_json=config.input_json,
            entities_output=config.entities_output,
            relationships_output=config.relationships_output,
            triples_output=config.triples_output,
            neo4j_uri=config.neo4j.uri,
            neo4j_username=config.neo4j.username,
            neo4j_password=config.neo4j.password,
        )
    def get_embedding_pipeline_config(self) -> EmbeddingPipelineConfig:
        config = self.config.embedding_pipeline

        return EmbeddingPipelineConfig(
            input_json=config.input_json,
            chunking=ChunkingConfig(chunk_size=config.chunking.chunk_size,
                                    chunk_overlap=config.chunking.chunk_overlap),
            embedding_model=EmbeddingModelConfig(name = config.embedding_model.name,
                                                embedding_dim=config.embedding_model.embedding_dim),
            vector_store=VectorStoreConfig(type = config.vector_store.type,
                                        index_type=config.vector_store.index_type,
                                        index_path = config.vector_store.index_path,
                                        metadata_path= config.vector_store.metadata_path)
        )
    
    def get_rag_pipeline_config(self)->Ragpipelineconfig:
        config = self.config.rag

        return Ragpipelineconfig(
            input_json = config.input_json,
            faiss = faiss_data(index_path = config.faiss.index_path,
                        metadata_path = config.faiss.metadata_path,
                        top_k = config.faiss.top_k),
            neo4j = neo4j_config(uri = config.neo4j.uri,
                        username = config.neo4j.username,
                        password = config.neo4j.password),
            llm = llmconfig(provider = config.llm.provider,
                        model = config.llm.model,
                        temperature = config.llm.temperature,
                        max_tokens = config.llm.max_tokens)  
        )