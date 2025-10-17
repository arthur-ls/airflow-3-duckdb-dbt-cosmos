from task.extract.get_data import ImportData
from task.save.save_data import DuckDBSaveDatabase
from utils.yaml.yaml_reader import read_yaml_file

def operator_executor(yaml_path: str):
    yaml_data = read_yaml_file(yaml_path)
    print(yaml_data)

    import_data = ImportData(kaggle_project=yaml_data['kaggle_project'])
    db_connect = DuckDBSaveDatabase(db_path=yaml_data['database'])
    db_connect.create_schema(yaml_data['schema'])

    for file in yaml_data['file_path']:
        table_name = file.split('.')[0].replace('-', '_')
        df = import_data.get_kaggle_data(file_path=file)
        db_connect.save_data(schema_name=yaml_data['schema'], table_name=table_name, df=df)
        db_connect.check_data_table(schema_name=yaml_data['schema'], table_name=table_name)

    db_connect.close_connection()