from src.knowledge_graph.config.configuration import ConfigManager
from src.knowledge_graph.components.data_transformation import DataTransformation
from src.knowledge_graph.exception.exception import KGException
import sys

class DataTransformationTrainingPipeline:
    def __init__(self):
        pass

    def initiate_data_transformation(self):
        try:
            config = ConfigManager()
            config = config.get_data_transformation_config()
            obj = DataTransformation(config)
            obj.extract_entities()
            obj.extract_relationships()
            obj.create_triples()
            obj.build_graph()
        except Exception as e :
            raise KGException(e,sys)