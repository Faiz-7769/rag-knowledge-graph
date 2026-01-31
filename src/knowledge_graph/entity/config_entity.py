from dataclasses import dataclass
from pathlib import Path

#data ingestion part
@dataclass
class DataIngestionConfig:
    root_dir: Path
    email_dir: Path
    pdf_dir: Path
    csv_dir: Path
    db_path: Path
    output_json: str

#datatransformation part
@dataclass
class DataTransformationConfig:
    input_json: Path
    entities_output: Path
    relationships_output: Path
    triples_output: Path
    neo4j_uri: str
    neo4j_username: str
    neo4j_password: str

#data embedding part
@dataclass
class ChunkingConfig:
    chunk_size: int
    chunk_overlap: int

@dataclass
class EmbeddingModelConfig:
    name: str
    embedding_dim: int

@dataclass
class VectorStoreConfig:
    type: str
    index_type: str
    index_path: Path
    metadata_path: Path

@dataclass
class EmbeddingPipelineConfig:
    input_json: Path
    chunking: ChunkingConfig
    embedding_model: EmbeddingModelConfig
    vector_store: VectorStoreConfig

#Rag part
@dataclass
class faiss_data:
    index_path: Path
    metadata_path: Path
    top_k: int

@dataclass
class llmconfig:
    provider: str
    model: str
    temperature: float
    max_tokens: int

@dataclass
class neo4j_config:
    uri: str
    username: str
    password: str

@dataclass
class Ragpipelineconfig:
    input_json: Path
    faiss: faiss_data
    neo4j: neo4j_config
    llm: llmconfig