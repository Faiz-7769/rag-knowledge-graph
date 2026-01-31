from src.knowledge_graph.logger.logging import logger
from src.knowledge_graph.exception.exception import KGException
from src.knowledge_graph.pipeline.stage_1 import DataIngestionTrainingPipeline
from src.knowledge_graph.pipeline.stage_2 import DataTransformationTrainingPipeline
from src.knowledge_graph.pipeline.stage_3 import DataEmbeddingPipeline
import sys

STAGE_NAME = "Data Ingestion"

try:
    logger.info("Initiailizing Data Ingestion Pipeline")
    obj = DataIngestionTrainingPipeline()
    obj.initiate_data_ingestion()
    logger.info("Completed Data Ingestion Pipeline")
except Exception as e:
    raise KGException(e,sys)

STAGE_NAME = " Data Transformation"

try:
    logger.info("Initializing Data Transformation Pipeline")
    obj = DataTransformationTrainingPipeline()
    obj.initiate_data_transformation()
    logger.info("Completed Data Transformation Pipeline")
except Exception as e:
    raise KGException(e,sys)

STAGE_NAME ="Data Embedding"

try:
    logger.info("Inititalizing Data Embedding")
    obj = DataEmbeddingPipeline()
    obj.initiate_data_embedding()
    logger.info("Completed Data Embedding")
except Exception as e:
    raise KGException(e,sys)
