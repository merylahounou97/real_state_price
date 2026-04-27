from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import json
import requests

# --- CONFIGURATION ---
LOCAL_DATA_DIR = "/c/Users/ahoun/OneDrive/Documents/Documents/real_state_price/src/data"
NETWORK_NAME = "real_state_price_data-stack"

default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# --- SPEC D'INGESTION DRUID  ---
druid_ingestion_spec = {
    "type": "index_parallel",
    "spec": {
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {
                "type": "local",
                "baseDir": "/opt/druid/var/druid/data/ml/clustering/",
                "filter": "**.parquet", 
            },
            "inputFormat": {
                "type": "parquet"
            },
        },
        "dataSchema": {
            "dataSource": "real_estate_clusters",
            "timestampSpec": {
                "column": "date_inventaire",
                "format": "auto", 
                "missingValue": "2000-01-01",
            },
            "dimensionsSpec": {
                "dimensions": [
                    "type",
                    "fonction",  
                    "region",
                    "dept",     
                    "ministere",
                    {
                        "type": "long",
                        "name": "prediction", 
                    },
                ]
            },
            "metricsSpec": [
                {"type": "count", "name": "total_biens"}
            ],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "YEAR",
                "queryGranularity": "MONTH",
                "rollup": False,
                "intervals": ["2000-01-01/2035-01-01"],
            },
        },
        "tuningConfig": {
            "type": "index_parallel",
            "maxNumConcurrentSubTasks": 1,
            "logParseExceptions": True,
        },
    },
}

# --- SENSOR POUR SURVEILLER L'INGESTION ---
class DruidTaskSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, druid_url: str, xcom_dag_task_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.druid_url = druid_url
        self.xcom_dag_task_id = xcom_dag_task_id

    def poke(self, context):
        raw_response = context["ti"].xcom_pull(task_ids=self.xcom_dag_task_id)
        if not raw_response:
            return False

        try:
            task_id = json.loads(raw_response).get("task")
            url = f"{self.druid_url}/druid/indexer/v1/task/{task_id}/status"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            status = resp.json().get("status", {}).get("status")
            
            self.log.info(f"Statut de l'ingestion Druid ({task_id}) : {status}")
            
            if status == "SUCCESS":
                return True
            if status in ("FAILED", "CANCELED"):
                raise Exception(f"L'ingestion Druid a échoué avec le statut : {status}")
            return False
        except Exception as e:
            self.log.error(f"Erreur polling Druid : {e}")
            return False

# --- DÉFINITION DU WORKFLOW ---
with DAG(
    dag_id="pipeline_immobilier_etat",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2026, 4, 1),
    catchup=False,
) as dag:

    common_params = {
        "image": "immobilier-app:latest",
        "docker_url": "unix://var/run/docker.sock",
        "network_mode": NETWORK_NAME,
        "auto_remove": True,
        "mounts": [{"source": LOCAL_DATA_DIR, "target": "/src/data", "type": "bind"}],
    }

    t1_scraping = DockerOperator(
        task_id="scraping_data",
        command="pipenv run python scripts/scraping.py",
        **common_params,
    )

    t2_spark_clean = DockerOperator(
        task_id="spark_processing",
        command="pipenv run python scripts/data_processing_spark.py",
        **common_params,
    )

    t3_ml_clustering = DockerOperator(
        task_id="ml_clustering",
        command="pipenv run python scripts/real_estate_ml.py",
        **common_params,
    )

    t4_wait_druid_api = HttpSensor(
        task_id="wait_for_druid_overlord",
        http_conn_id="druid_api",
        endpoint="/druid/indexer/v1/leader",
        poke_interval=15,
        timeout=300,
    )

    t5_ingestion_druid = SimpleHttpOperator(
        task_id="ingestion_druid",
        http_conn_id="druid_api",
        endpoint="/druid/indexer/v1/task",
        method="POST",
        data=json.dumps(druid_ingestion_spec),
        headers={"Content-Type": "application/json"},
        do_xcom_push=True,
    )

    t6_verify_ingestion = DruidTaskSensor(
        task_id="wait_ingestion_druid",
        druid_url="http://druid:8081", # Overlord
        xcom_dag_task_id="ingestion_druid",
        poke_interval=20,
        timeout=1800,
        mode="reschedule",
    )

    # Dépendances
    t1_scraping >> t2_spark_clean >> t3_ml_clustering >> t4_wait_druid_api >> t5_ingestion_druid >> t6_verify_ingestion