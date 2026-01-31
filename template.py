import os

project_name = "knowledge_graph"

list_of_files = [
    "config/config.yaml",
    "schema/schema.yaml",
    "setup.py",
    "README.md",
    "main.py",

    "src/knowledge_graph/__init__.py",
    "src/knowledge_graph/components/data_ingestion.py",
    "src/knowledge_graph/components/__init__.py",
    "src/knowledge_graph/pipeline/__init__.py",
    "src/knowledge_graph/pipeline/stage_1.py",
    "src/knowledge_graph/config/__init__.py",
    "src/knowledge_graph/config/configuration.py",
    "src/knowledge_graph/entity/__init__.py",
    "src/knowledge_graph/entity/config_entity.py",
    "src/knowledge_graph/utils/__init__.py",
    "src/knowledge_graph/utils/common.py",
    "src/knowledge_graph/logger/__init__.py",
    "src/knowledge_graph/logger/logging.py",
    "src/knowledge_graph/exception/exception.py",
    "src/knowledge_graph/exception/__init__.py",

]

for filepath in list_of_files:
    filepath = os.path.join(os.getcwd(), filepath)
    filedir, filename = os.path.split(filepath)

    if filedir != "":
        os.makedirs(filedir, exist_ok=True)

    if not os.path.exists(filepath):
        with open(filepath, "w"):
            pass
