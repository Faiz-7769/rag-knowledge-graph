import os
import faiss
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

from langchain_groq import ChatGroq

from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
import spacy

from src.knowledge_graph.components.data_retriever import HybridRetriever
from src.knowledge_graph.config.configuration import ConfigManager
from src.knowledge_graph.logger.logging import logger
from src.knowledge_graph.utils.common import read_json
from dotenv import load_dotenv
load_dotenv()

class RAGPipeline:
        @staticmethod
        def get_rag_chain():
            # 1. Load Config
            config = ConfigManager().get_rag_pipeline_config()

            # 2. Load Resources (Embeddings, FAISS, Graph)
            # Note: We still use SentenceTransformer for embeddings locally to match your FAISS index
            embedder = SentenceTransformer('all-MiniLM-L6-v2') 

            index = faiss.read_index(config.faiss.index_path)
            metadata = read_json(config.faiss.metadata_path)
                
            graph = GraphDatabase.driver(
                os.getenv("NEO_4J_URI"),
                auth=(config.neo4j.username, os.getenv("PASSWORD"))
            )

            nlp = spacy.load("en_core_web_sm")

            # 3. Initialize Retriever
            retriever = HybridRetriever(
                vector_index=index,
                vector_metadata=metadata,
                embedder=embedder,
                graph=graph,
                nlp=nlp,
                top_k_vector= config.faiss.top_k,
                top_k_graph= 5
            )
            logger.info("LLM Initialzed successfully")
            llm = ChatGroq(
                model=config.llm.model,
                groq_api_key=os.getenv("GROQ_API_KEY"),
                temperature=config.llm.temperature,
                max_tokens=config.llm.max_tokens
                )

            # 5. Prompt & Chain
            template = """You are a helpful assistant.If you get any context
            use that to provide answer to the question with full confidence.Always denote database as my database.
            If you do not have any context,provide an overview of the 
            question based on your general knowledge and end with "As an AI language model, I do not have the information in my Database".
            context given below  

            Context:
            {context}

            Question: {question}
            """
            prompt = ChatPromptTemplate.from_template(template)

            setup_and_retrieval = RunnableParallel(
            {"context": retriever, "question": RunnablePassthrough()}
            )

            answer_generation = (
            prompt 
            | llm 
            | StrOutputParser()
            )

        # Step C: Combine them so we return BOTH the Answer AND the Original Context
            chain = (
            setup_and_retrieval
            |  RunnableParallel({
                "result": answer_generation,          # The Text Answer
                "source_documents": lambda x: x["context"] # The Original Docs (Preserved!)
            })
            )
            logger.info("RAG Chain created successfully")
            return chain