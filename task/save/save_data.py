import duckdb
import pandas as pd
from utils.treatments.exception import try_except

class DuckDBSaveDatabase:
    def __init__(self, db_path: str = ":memory:"):
        self.conn = duckdb.connect(database=db_path)
        self._db_path = db_path

    @try_except
    def connect_persistent_db(self):
        if self._db_path != ':memory':
            self.conn.execute(f"ATTACH '{self._db_path}' AS db;").fetchall()
            self.conn.execute(f"USE {self._db_path};").fetchall()

    @try_except
    def create_schema(self, schema):
        result = self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};").fetchall()
        return result

    @try_except
    def save_data(self, schema_name: str, table_name: str, df: pd.DataFrame):
        table_df = df
        result = self.conn.execute(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} AS SELECT * FROM table_df;").fetchall()
        return result

    @try_except
    def validate_table_existence(self, schema_name: str, table_name: str):
        self.logger.info(f"Validating table existence")
        result = self.conn.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = {schema_name} AND table_name = '{table_name}');").fetchone()
        self.logger.info(f"Validation finished: {result}")

    @try_except
    def check_data_table(self, schema_name: str, table_name: str):
        result = self.conn.execute(f"SELECT * FROM {schema_name}.{table_name} LIMIT 100;").fetch_df()
        return result

    @try_except
    def close_connection(self):
        self.conn.close()
        