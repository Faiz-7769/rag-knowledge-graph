import os
import faiss
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
from langchain_text_splitters import RecursiveCharacterTextSplitter

from src.knowledge_graph.utils.common import read_json, write_json
from src.knowledge_graph.logger.logging import logger
from src.knowledge_graph.exception.exception import KGException
import sys

class DataEmbedding:
    """
    Milestone-3: Data Embedding Pipeline
    
    Optimized for RAG:
    - Preserves text content in metadata (Essential for retrieval).
    - Uses GPU acceleration if available.
    - Implements batched processing.
    """

    def __init__(self, config):
        try:
            self.config = config
            
            # 1. Load Data
            self.documents = read_json(self.config.input_json)
            
            # 2. Setup Device (GPU/CPU)
            self.device = "cuda" if torch.cuda.is_available() else "cpu"
            logger.info(f"Embedding Model will run on: {self.device.upper()}")

            # 3. Load Model
            self.model = SentenceTransformer(
                self.config.embedding_model.name,
                device=self.device
            )

            # 4. Setup Text Splitter
            self.text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=self.config.chunking.chunk_size,
                chunk_overlap=self.config.chunking.chunk_overlap,
                length_function=len,
                is_separator_regex=False,
            )

            # 5. Output Paths
            self.index_path = self.config.vector_store.index_path
            self.metadata_path = self.config.vector_store.metadata_path

            # Runtime Storage
            self.text_chunks = []
            self.metadata = []

        except Exception as e:
            raise KGException(e, sys)

    def prepare_chunks(self):
        """
        Step 1: Chunking
        Splits text and prepares metadata.
        CRITICAL: We must store the actual text in metadata for RAG retrieval.
        """
        logger.info("Starting text chunking...")
        
        try:
            global_chunk_id = 0
            
            for doc in self.documents:
                # Handle potentially missing text
                raw_text = doc.get("text", "")
                if not raw_text:
                    continue
                    
                chunks = self.text_splitter.split_text(raw_text)

                for chunk in chunks:
                    self.text_chunks.append(chunk)
                    
                    # Store rich metadata including the text itself
                    self.metadata.append({
                        "id": global_chunk_id,
                        "document_id": doc.get("id"),
                        "source_name": doc.get("source_name"),
                        "source_type": doc.get("source_type"),
                        "text": chunk,  # <--- CRITICAL FOR RAG
                        "created_at": doc.get("ingestion_timestamp")
                    })
                    global_chunk_id += 1

            logger.info(f"Chunking complete. Generated {len(self.text_chunks)} chunks.")

        except Exception as e:
            raise KGException(e, sys)

    def generate_embeddings(self):
        """
        Step 2: Vectorization
        Generates embeddings using the loaded model.
        """
        logger.info("Starting embedding generation...")
        
        try:
            if not self.text_chunks:
                logger.warning("No text chunks to embed.")
                return np.array([])

            # Encode in batches to manage memory
            batch_size = 32
            embeddings = self.model.encode(
                self.text_chunks,
                batch_size=batch_size,
                show_progress_bar=True,
                convert_to_numpy=True,
                normalize_embeddings=True # Good for cosine similarity search
            )
            
            logger.info(f"Generated embeddings with shape: {embeddings.shape}")
            return embeddings

        except Exception as e:
            raise KGException(e, sys)

    def save_vector_store(self, embeddings):
        """
        Step 3: Storage (FAISS + Metadata)
        """
        logger.info("Saving Vector Store and Metadata...")
        
        try:
            # --- A. Save Metadata ---
            # We save metadata as a JSON list where index i corresponds to vector i
            os.makedirs(os.path.dirname(self.metadata_path), exist_ok=True)
            write_json(self.metadata_path, self.metadata)
            
            # --- B. Save FAISS Index ---
            if len(embeddings) > 0:
                dimension = embeddings.shape[1]
                
                # Using FlatL2 (Euclidean Distance). 
                # For huge datasets (>100k), consider IndexIVFFlat.
                index = faiss.IndexFlatL2(dimension)
                index.add(embeddings)
                
                os.makedirs(os.path.dirname(self.index_path), exist_ok=True)
                faiss.write_index(index, self.index_path)
                
                logger.info(f"FAISS index saved to {self.index_path}")
            else:
                logger.warning("No embeddings to save.")

        except Exception as e:
            raise KGException(e, sys)
    
    def show_faiss_index(self):
        index = faiss.read_index(self.index_path)
        print("Total vectors:", index.ntotal)
        print("Vector dimension:", index.d)

