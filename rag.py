from src.knowledge_graph.logger.logging import logger
from src.knowledge_graph.exception.exception import KGException
from src.knowledge_graph.pipeline.rag_pipeline import RAGPipeline
import sys

try:
    logger.info("Initializing RAG Pipeline")
    rag_chain = RAGPipeline.get_rag_chain()
    logger.info("RAG Pipeline initialized successfully")
    question = input("Enter your question: ")
    response= rag_chain.invoke(question)
    print(f"Answer: {response}")
    logger.info("RAG Pipeline Executed successfully")
except Exception as e:
    raise KGException(e,sys)