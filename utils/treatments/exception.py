from utils.log.get_logging import Logger

def try_except(func):
    action = f"{func.__name__}".replace('_', ' ')
    file_path = func.__globals__['__name__']
    logger = Logger(f"{file_path}.{func.__name__}").get_logger()
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Initializing to {action}")
            result = func(*args, **kwargs)
            logger.info(f"Success to {action}: {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to {action}: {e}")
    return wrapper