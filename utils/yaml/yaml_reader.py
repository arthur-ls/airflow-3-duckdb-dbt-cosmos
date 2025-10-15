import yaml
from utils.log.get_logging import Logger
from glob import iglob

def get_yaml_paths(yaml_folder: str):
    return list(iglob(yaml_folder))

def read_yaml_file(file_path: str):
    """
    Reads a YAML file from the given path and returns its content as a Python dictionary.
    """
    logger = Logger(__name__).get_logger()
    try:
        with open(file_path, 'r') as file:
            data = yaml.load(file, Loader=yaml.SafeLoader)
        return data
    except FileNotFoundError:
        logger.error(f"Error: File not found at '{file_path}'")
        return None
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        return None