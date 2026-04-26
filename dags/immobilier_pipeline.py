from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import json
import requests

# --- CONFIGURATION ---
LOCAL_DATA_DIR = "/c/Users/ahoun/OneDrive/Documents/Documents/real_state_price/src/data"
NETWORK_NAME = "real_state_price_data-stack" 

default_args = {
    'owner': 'admin',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# --- SPEC D'INGESTION DRUID (Optimisée) ---
druid_ingestion_spec = {
    "type": "index_parallel",
    "spec": {
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {
                "type": "local",
                "baseDir": "/opt/druid/var/druid/data/ml/clustering/",
                "filter": "**.parquet" 
            },
            "inputFormat": {"type": "parquet"}
        },
        "dataSchema": {
            "dataSource": "real_estate_clusters",
            "timestampSpec": {
                "column": "date_inventaire",
                "format": "yyyy-MM", 
                "missingValue": "2000-01-01"
            },
            "dimensionsSpec": {
                "dimensions": [
                    "type",
                    "region",
                    "dept",
                    "ministere",
                    {"type": "long", "name": "prediction"} 
                ]
            },
            "metricsSpec": [
                {"type": "count", "name": "count"},
                {"type": "longSum", "name": "total_biens", "fieldName": "quantite"}
            ],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "YEAR",
                "queryGranularity": "MONTH",
                "rollup": False
            }
        },
        "tuningConfig": {
            "type": "index_parallel",
            "maxNumConcurrentSubTasks": 1 
        }
    }
}

# --- CUSTOM SENSOR POUR DRUID ---
class DruidTaskSensor(BaseSensorOperator):
    """Vérifie l'état réel de la tâche d'ingestion sur l'Overlord Druid"""
    @apply_defaults
    def __init__(self, druid_url: str, xcom_dag_task_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.druid_url = druid_url
        self.xcom_dag_task_id = xcom_dag_task_id

    def poke(self, context):
        # Récupération du taskId envoyé par SimpleHttpOperator
        raw_response = context["ti"].xcom_pull(task_ids=self.xcom_dag_task_id)
        if not raw_response:
            return False
        
        task_id = json.loads(raw_response).get("task")
        url = f"{self.druid_url}/druid/indexer/v1/task/{task_id}/status"
        
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            status = resp.json().get("status", {}).get("status")
            self.log.info(f"Druid Task {task_id} status: {status}")
            
            if status == "SUCCESS":
                return True
            elif status in ("FAILED", "CANCELED"):
                raise Exception(f"L'ingestion Druid a échoué (Status: {status})")
            return False 
        except Exception as e:
            self.log.error(f"Erreur lors du polling Druid: {e}")
            return False

# --- DÉFINITION DU DAG ---
with DAG(
    'pipeline_immobilier_etat',
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2026, 4, 1),
    catchup=False,
) as dag:

    common_docker_params = {
        "image": 'immobilier-app:latest',
        "docker_url": 'unix://var/run/docker.sock',
        "network_mode": NETWORK_NAME,
        "auto_remove": True,
        "mounts": [{"source": LOCAL_DATA_DIR, "target": "/src/data", "type": "bind"}]
    }

    # 1. Scraping
    scraping = DockerOperator(
        task_id='scraping_data', 
        command='pipenv run python scripts/scraping.py', 
        **common_docker_params
    )

    # 2. Spark Processing
    spark_clean = DockerOperator(
        task_id='spark_processing', 
        command='pipenv run python scripts/data_processing_spark.py', 
        **common_docker_params
    )

    # 3. Machine Learning (Clustering)
    ml_clustering = DockerOperator(
        task_id='ml_clustering', 
        command='pipenv run python scripts/real_estate_ml.py', 
        **common_docker_params
    )

    # 4. Envoi de la spec à Druid (Asynchrone)
    index_in_druid = SimpleHttpOperator(
        task_id='ingestion_druid',
        http_conn_id='druid_api',
        endpoint='/druid/indexer/v1/task',
        method='POST',
        data=json.dumps(druid_ingestion_spec),
        headers={"Content-Type": "application/json"},
        do_xcom_push=True 
    )

    # 5. Attente du succès réel de Druid
    wait_for_ingestion = DruidTaskSensor(
        task_id='wait_ingestion_druid',
        druid_url="http://druid:8081", 
        xcom_dag_task_id='ingestion_druid',
        poke_interval=20,
        timeout=600,
        mode="reschedule"
    )

    # Dépendances
    scraping >> spark_clean >> ml_clustering >> index_in_druid >> wait_for_ingestion