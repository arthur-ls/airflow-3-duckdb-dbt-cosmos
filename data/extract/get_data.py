import kagglehub
from kagglehub import KaggleDatasetAdapter
from pandas import DataFrame
from utils.log.get_logging import Logger

class ImportData:
    def __init__(self, kaggle_project:str, file_path:str):
        self._kaggle_project = kaggle_project
        self._file_path = file_path
        self.logger = Logger(__name__).get_logger()

    def get_data(self) -> DataFrame:
        try:
            self.logger.info(f"Fetching data from Kaggle project: {self._kaggle_project}")
            df = kagglehub.dataset_load(
                KaggleDatasetAdapter.PANDAS,
                self._kaggle_project,
                self._file_path,
            )
            self.logger.info("Data fetched successfully")
            return df
        except Exception as e:
            self.logger.error(f"Error fetching data: {e}")
            raise