from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import json


# CONFIGURATION DES CHEMINS 

LOCAL_DATA_DIR = "/c/Users/ahoun/OneDrive/Documents/Documents/real_state_price/src/data"

# Configuration par défaut
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Spec d'ingestion pour Druid
druid_ingestion_spec = {
    "type": "index_parallel",
    "spec": {
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {
                "type": "local",
                "baseDir": "/opt/druid/var/druid/data/ml/clustering/",
                "filter": "*.parquet"
            },
            "inputFormat": {"type": "parquet"}
        },
        "dataSchema": {
            "dataSource": "real_estate_clusters",
            "timestampSpec": {"column": "date_inventaire", "format": "auto"},
            "dimensionsSpec": {
                "dimensions": ["type", "region", "dept", "ministere", {"type": "long", "name": "prediction"}]
            },
            "metricsSpec": [
                {"type": "longSum", "name": "total_biens", "fieldName": "quantite"}
            ],
            "granularitySpec": {
                "type": "uniform",
                "segmentGranularity": "YEAR",
                "rollup": False
            }
        }
    }
}

# Définition du DAG
with DAG(
    'pipeline_immobilier_etat',
    default_args=default_args,
    description='Pipeline complet : Scraping -> Spark -> ML -> Druid',
    schedule_interval=None, 
    start_date=datetime(2026, 4, 1),
    catchup=False,
) as dag:

    # 1. SCRAPING
    # Cette étape crée le fichier parquet initial
    scraping = DockerOperator(
        task_id='scraping_data',
        image='immobilier-app:latest',
        command='pipenv run python scripts/scraping.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mounts=[{
            "source": LOCAL_DATA_DIR, 
            "target": "/src/data", 
            "type": "bind"
        }]
    )

    # 2. SPARK PROCESSING
    # Cette étape lit le parquet et le nettoie
    spark_clean = DockerOperator(
        task_id='spark_processing',
        image='immobilier-app:latest',
        command='pipenv run python scripts/data_processing_spark.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mounts=[{
            "source": LOCAL_DATA_DIR, 
            "target": "/src/data", 
            "type": "bind"
        }]
    )

    # 3. MACHINE LEARNING
    # Cette étape effectue le clustering
    ml_clustering = DockerOperator(
        task_id='ml_clustering',
        image='immobilier-app:latest',
        command='pipenv run python scripts/real_estate_ml.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        auto_remove=True,
        mounts=[{
            "source": LOCAL_DATA_DIR, 
            "target": "/src/data", 
            "type": "bind"
        }]
    )

    # 4. INGESTION DRUID (Via API)
    index_in_druid = SimpleHttpOperator(
        task_id='ingestion_druid',
        http_conn_id='druid_api',
        endpoint='/druid/indexer/v1/task',
        method='POST',
        data=json.dumps(druid_ingestion_spec),
        headers={"Content-Type": "application/json"},
    )

    # Ordre d'exécution
    scraping >> spark_clean >> ml_clustering >> index_in_druid