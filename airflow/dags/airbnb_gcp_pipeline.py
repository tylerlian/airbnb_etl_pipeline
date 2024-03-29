from datetime import datetime
import logging
import os
import os.path as osp
import pandas as pd
import zipfile
import kaggle
from kaggle.api.kaggle_api_extended import KaggleApi

from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow.providers.google.cloud.operators.dataproc
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator, DataprocSubmitJobOperator )


from google.cloud import storage, bigquery
import logging

# from bigquery_utils import load_csv_bq, schema_dict

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
GCP_BQ_TABLE = os.environ.get("GCP_BQ_TABLE")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
REGION = "us-west1"
ZONE = "us-west1-a"
CLUSTER_NAME = "airbnb-dproc-cluster"

PYSPARK_MAIN_FILE = "pyspark_script.py"
PYSPARK_MAIN_FILE_PATH = os.path.join(AIRFLOW_HOME, "dags", PYSPARK_MAIN_FILE)
SPARK_BQ_JAR = "spark-bigquery-latest_2.12.jar"
SPARK_BQ_JAR_PATH = os.path.join(AIRFLOW_HOME, "dags", SPARK_BQ_JAR)

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
current_date = datetime.now()

import kaggle 

CLUSTER_GENERATOR_CONFIG = ClusterGenerator(
            project_id=PROJECT_ID,
            zone=ZONE,
            master_machine_type="n1-standard-4",
            idle_delete_ttl=900,                    
            master_disk_size=500,
            num_masters=1, # single node cluster
            num_workers=0,                     
        ).make()


pyspark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/{PYSPARK_MAIN_FILE}",
        "jar_file_uris": [f"gs://{BUCKET}/{SPARK_BQ_JAR}"],
        "args": [
            f"--project_id={PROJECT_ID}",
            f"--bq_table_input={GCP_BQ_TABLE}",
            f"--bucket={BUCKET}"
        ],
    },
}

AIRBNB_SCHEMA = {
    "id": "INTEGER",
    "name": "STRING",
    "host_id": "INTEGER",
    "host_name": "STRING",
    "neighbourhood_group": "STRING",
    "neighbourhood": "STRING",
    "latitude": "FLOAT",
    "longitude": "FLOAT",
    "room_type": "STRING",
    "price": "INTEGER",
    "minimum_nights": "INTEGER",
    "number_of_reviews": "INTEGER",
    "last_review": "DATE",
    "reviews_per_month": "FLOAT",
    "calculated_host_listings_count": "INTEGER",
    "availability_365": "INTEGER"
}

def upload_to_gcs(local_file, bucket):
    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = os.path.basename(local_file)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def bq_schema_setup(schema):
    bq_schema = []
    for field, type in schema.items():
        bq_schema.append(bigquery.SchemaField(field, type))

def bucket_to_bq(ds, **kwargs):
    current_date_str = current_date.strftime("%Y-%m-%d")
    bucket_file = f"gs://{BUCKET}/airbnb_data/{current_date_str}.csv"
    table_id = f"{PROJECT_ID}.airbnb_data.{GCP_BQ_TABLE}"
    bq_schema = [bigquery.SchemaField(field, dtype) for field, dtype in AIRBNB_SCHEMA.items()]

    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        skip_leading_rows=1,
        max_bad_records=100000000
    )

    load_job = client.load_table_from_uri(
        bucket_file, table_id, job_config=job_config
    )

    load_job.result()

def get_data_kaggle(ds, **kwargs):
    current_date_str = current_date.strftime("%Y-%m-%d")
    # os.system('kaggle datasets download -d dgomonov/new-york-city-airbnb-open-data')
    # with zipfile.ZipFile('new-york-city-airbnb-open-data.zip', 'r') as zObject:
    #     zObject.extract('AB_NYC_2019.csv', path=f"./dags/data/{current_date_str}")
    # zObject.close()
    dataset = 'dgomonov/new-york-city-airbnb-open-data'
    path = f'./dags/data/{current_date_str}/'
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_file(dataset, file_name='AB_NYC_2019.csv', path=path)
    with zipfile.ZipFile(f"./dags/data/{current_date_str}/AB_NYC_2019.csv.zip", 'r') as zObject:
        zObject.extract('AB_NYC_2019.csv', path=f"./dags/data/{current_date_str}")
    zObject.close()
    os.system(f"rm ./dags/data/{current_date_str}/AB_NYC_2019.csv.zip")
    

def upload_data_to_bucket(bucket, ds, **kwargs):

    current_date_str = current_date.strftime("%Y-%m-%d")
    file_name = f"{current_date_str}.csv"
    local_file = f"./dags/data/{current_date_str}/AB_NYC_2019.csv"
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    object_name = f"airbnb_data/{file_name}"
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 1,
}

with DAG(
    dag_id="data_airbnb_dag",
    schedule_interval="5 0 3 1,4,7,10 *",
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
) as dag:

    download_dataset_task = PythonOperator(
        task_id="download_dataset",
        python_callable=get_data_kaggle,
        provide_context=True,
    )

    local_to_bucket_task = PythonOperator(
        task_id="local_to_bucket_task",
        python_callable=upload_data_to_bucket,
        provide_context=True,
        op_kwargs={
            "bucket": BUCKET,
        },
    )

    bucket_to_bq_task = PythonOperator(
        task_id="bucket_to_bq_task",
        python_callable=bucket_to_bq,
        provide_context=True,
    )

    upload_pyspark_script = PythonOperator(
        task_id="upload_pyspark_script",
        python_callable=upload_to_gcs,
        op_kwargs={
            "local_file": PYSPARK_MAIN_FILE_PATH,
            "bucket": BUCKET,
        },
    )

    upload_jar = PythonOperator(
        task_id="upload_jar",
        python_callable=upload_to_gcs,
        op_kwargs={
            "local_file": SPARK_BQ_JAR_PATH,
            "bucket": BUCKET,
        },
    )

    create_cluster_operator_task = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_config=CLUSTER_GENERATOR_CONFIG
    )
    
    pyspark_task = DataprocSubmitJobOperator(
        task_id="pyspark_task", job=pyspark_job, region=REGION, project_id=PROJECT_ID
    )

    download_dataset_task >> local_to_bucket_task >> bucket_to_bq_task >> upload_pyspark_script  >> upload_jar >> create_cluster_operator_task >> pyspark_task