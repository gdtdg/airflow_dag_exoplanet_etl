import time
from datetime import datetime

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.models.connection import Connection
from airflow.operators.python_operator import PythonOperator

from ExoplanetDataRepository import ExoplanetDataRepository
from ExoplanetElasticSearchRepository import ExoplanetElasticSearchRepository
from TransformService import TransformService


def main(api_url, es_index, connection):
    requests.packages.urllib3.disable_warnings()
    start = time.time()

    print("Process started")
    exoplanet_data_repo = ExoplanetDataRepository(api_url)
    exoplanet_elasticseach_repo = ExoplanetElasticSearchRepository(es_index, connection)
    exoplanet_elasticseach_repo.delete_es_index()
    print("Index deleted")
    exoplanet_elasticseach_repo.create_index_with_mapping()
    print("Index with mapping created")
    transform = TransformService(exoplanet_data_repo, exoplanet_elasticseach_repo)
    transform.process()
    print("All rows transformed")

    end = time.time()
    elapsed = end - start
    return f"DAG 'etl_exoplanet' done in {elapsed:.2f} seconds."


dag = DAG('etl_exoplanet', description='ETL exoplanet',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

etl_exoplanet_operator = PythonOperator(task_id='etl_exoplanet',
                                        python_callable=main,
                                        dag=dag,
                                        op_kwargs={
                                            "api_url": Variable.get("api_url"),
                                            "es_index": Variable.get("es_index"),
                                            "connection": Connection.get_connection_from_secrets(
                                                "elasticsearch_exoplanet")
                                        })

etl_exoplanet_operator
