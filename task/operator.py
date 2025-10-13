from task.extract.get_data import ImportData
from task.save.save_data import DuckDBSaveDatabase
from utils.yaml.yaml_reader import read_yaml_file

class OperatorExecutor:
    def __init__(self, yaml_path: str):
        self._yaml_data = read_yaml_file(yaml_path)
        
    def get_yaml_data(self):
        yaml_data = self._yaml_data
        return yaml_data['database'], yaml_data['schema'], yaml_data['kaggle_project'], yaml_data['file_path']
    
    def insert_source_data_source(self):
        db, schema, kaggle_project, file_path = self.get_yaml_data()
        import_data = ImportData(kaggle_project=kaggle_project)
        db_connect = DuckDBSaveDatabase(db_path=db)
        db_connect.connect_persistent_db()
        db_connect.create_schema(schema)

        for file in file_path:
            table_name = file.split('.')[0].replace('-', '_')
            df = import_data.get_kaggle_data(file_path=file)
            db_connect.save_data(schema_name=schema, table_name=table_name, df=df)
            db_connect.check_data_table(schema_name=schema, table_name=table_name)

        db_connect.close_connection()