from task.operator import operator_executor
from utils.yaml.yaml_reader import get_yaml_paths
import os

YAML_FOLDER = os.path.dirname(__file__) + "/*.yaml"

for file in get_yaml_paths(YAML_FOLDER):
    operator_executor(yaml_path=file)