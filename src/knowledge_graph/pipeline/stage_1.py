from src.knowledge_graph.config.configuration import ConfigManager
from src.knowledge_graph.components.data_ingestion import DataIngestion
from src.knowledge_graph.exception.exception import KGException
import sys


class DataIngestionTrainingPipeline:
    def __init__(self):
        pass

    def initiate_data_ingestion(self):
        try:
            config = ConfigManager()
            di_config = config.get_data_ingestion_config()
            ingestion = DataIngestion(di_config)
            ingestion.ingest()
        except Exception as e:
            raise KGException(e,sys)