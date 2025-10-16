from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from airflow.providers.standard.operators.python import PythonOperator
from utils.yaml.yaml_reader import get_yaml_paths
from task.operator import operator_executor
from pathlib import Path
import os

YAML_FOLDER = os.path.dirname(__file__) + "/*.yml"
DBT_PATH = "/opt/airflow/dbt/dbt_duckdb_iceberg"
DBT_PROFILE = "dbt_duckdb_iceberg"
DBT_TARGETS = "dev"
MAX_ACTIVE_RUNS = 1
TAGS = ['DBT', 'Cosmos', 'Campeonato Brasileiro', 'Kaggle']
DAG_ID = 'dbt-kaggle-campeonato_brasileiro'

def get_dbt_configs():
    profile_config = ProfileConfig(
        profile_name=DBT_PROFILE,
        target_name=DBT_TARGETS,
        profiles_yml_filepath=Path(f"{DBT_PATH}/profiles.yml"),
    )

    project_config = ProjectConfig(
        dbt_project_path=DBT_PATH,
        models_relative_path="models"
    )
    return profile_config, project_config

def create_dag(file: str, project_config, profile_config):
    default_args = {
        "owner": "arthur.santos",
        "start_date": datetime(2022, 1, 1),
        "retries": 1,
    }

    dag = DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        schedule=None,
        catchup=False,
    )

    with dag:
        start_dag = EmptyOperator(task_id="start_dag", dag=dag)
        end_dag = EmptyOperator(task_id="end_dag", dag=dag)

        prepare_source_data = PythonOperator(
            task_id='prepare_source_data',
            python_callable=operator_executor,
            op_args=[f'{file}']
        )

        dbt_running_models = DbtTaskGroup(
           group_id="dbt_running_models",
           project_config=project_config,
           profile_config=profile_config,
           default_args={"retries": 2},
           dag=dag,
        )

        start_dag >> prepare_source_data >> dbt_running_models >> end_dag

        # start_dag >> prepare_source_data >> end_dag

    return dag

profile_config, project_config = get_dbt_configs()
for file in get_yaml_paths(YAML_FOLDER):
    create_dag(file, project_config=project_config, profile_config=profile_config)