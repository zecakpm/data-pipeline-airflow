from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os
import requests
import pandas as pd

default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eurostat_sdg_07_40_gcs_bq",
    default_args=default_args,
    schedule_interval="0 0 * * 1",  # Every Monday
    catchup=False,
    tags=["eurostat", "gcs"],
    params={
        "bucket": Variable.get("GCS_BUCKET_NAME"),          # 🔁 Replace this
        "prefix": "eurostat/sdg_07_40",       # 🔁 Customize if needed
    },
) as dag:

    def fetch_data(**kwargs):
        api_url = (
            "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/sdg_07_40/"
            "?format=JSON&lang=en&freq=A&nrg_bal=REN&unit=PC"
            "&geo=EU27_2020&geo=EU28&geo=EA20&geo=BE&geo=BG&geo=CZ&geo=DK&geo=DE"
            "&geo=EE&geo=IE&geo=EL&geo=ES&geo=FR&geo=HR&geo=IT&geo=CY&geo=LV&geo=LT"
            "&geo=LU&geo=HU&geo=MT&geo=NL&geo=AT&geo=PL&geo=PT&geo=RO&geo=SI&geo=SK"
            "&geo=FI&geo=SE&geo=IS&geo=NO&geo=CH&geo=UK&geo=BA&geo=ME&geo=MK&geo=AL"
            "&geo=RS&geo=XK"
            "&time=2004&time=2005&time=2006&time=2007&time=2008&time=2009&time=2010"
            "&time=2011&time=2012&time=2013&time=2014&time=2015&time=2016&time=2017"
            "&time=2018&time=2019&time=2020&time=2021&time=2022&time=2023"
        )

        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        geo_labels = data["dimension"]["geo"]["category"]["label"]
        time_labels = data["dimension"]["time"]["category"]["label"]
        values = data["value"]
        time_size = data["size"][4]

        geo_index = {v: k for k, v in data["dimension"]["geo"]["category"]["index"].items()}
        time_index = {v: k for k, v in data["dimension"]["time"]["category"]["index"].items()}

        records = []
        for idx_str, value in values.items():
            idx = int(idx_str)
            geo_pos = idx // time_size
            time_pos = idx % time_size
            geo_code = geo_index[geo_pos]
            time_code = time_index[time_pos]
            records.append({
                "Country_Code": geo_code,
                "Country": geo_labels[geo_code],
                "Year": time_labels[time_code],
                "Value": value,
            })

        df = pd.DataFrame(records)
        output_path = "/tmp/eurostat_sdg_07_40.csv"
        df.to_csv(output_path, index=False)
        kwargs["ti"].xcom_push(key="csv_path", value=output_path)

    def upload_to_gcs(**kwargs):
        ti = kwargs["ti"]
        file_path = ti.xcom_pull(task_ids="fetch_data", key="csv_path")
        if not file_path:
            raise ValueError("CSV path missing from XCom")

        bucket = kwargs["params"]["bucket"]
        prefix = kwargs["params"]["prefix"]
        execution_date = kwargs["ds"]
        gcs_path = f"{prefix}/{execution_date}.csv"

        # the following is use when running in a local environment, path add the to UI directly
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
        gcs_hook.upload(bucket_name=bucket, object_name=gcs_path, filename=file_path)

    fetch = PythonOperator(
        task_id="fetch_data",
        python_callable=fetch_data,
    )

    upload = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="{{ params.bucket }}",
        source_objects=["{{ params.prefix }}/{{ ds }}.csv"],
        destination_project_dataset_table="data-engineering-411419.raw_eurostat_dev.eurostat_sdg_07_40",  # 👈 adjust to your project
        schema_fields=[
            {"name": "Country_Code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Country", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Year", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Value", "type": "FLOAT", "mode": "NULLABLE"},
        ],
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # 🔁 Truncates table each time
        gcp_conn_id="google_cloud_default",
    )

    fetch >> upload >> load_to_bq