from task.operator import OperatorExecutor
from utils.yaml.yaml_reader import get_yaml_paths
import os

YAML_FOLDER = os.path.dirname(__file__) + "/*.yaml"

for file in get_yaml_paths(YAML_FOLDER):
    OperatorExecutor(yaml_path=file).insert_source_data_source()