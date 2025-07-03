from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
from airflow.models import Variable
import os
import requests
import pandas as pd
import numpy as np


default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="eurostat_nrg_bal_s_gcs_bq",
    default_args=default_args,
    schedule_interval="0 0 * * 1",  # Every Monday
    catchup=False,
    tags=["eurostat", "gcs"],
    params={
        "bucket": Variable.get("GCS_BUCKET_NAME"),         # 🔁 Replace this
        "prefix": "eurostat/nrg_bal_s",       # 🔁 Customize if needed
    },
) as dag:

    def fetch_data(**kwargs):

        api_url = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/nrg_bal_s/1.0/*.*.*.*.*?c[freq]=A&c[nrg_bal]=PPRD&c[siec]=TOTAL,C0000X0350-0370,C0350-0370,P1000,S2000,G3000,O4000XBIO,RA000,W6100_6220,N900H,E7000,H8000&c[unit]=KTOE&c[geo]=EU27_2020,BE,BG,CZ,DK,DE,EE,IE,EL,ES,FR,HR,IT,CY,LV,LT,LU,HU,MT,NL,AT,PL,PT,RO,SI,SK,FI,SE,IS,NO,BA,ME,MD,MK,GE,AL,RS,TR,UA,XK&c[TIME_PERIOD]=ge:2023&compress=false&format=json&lang=en"

        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()

        geo_labels = data['dimension']['geo']['category']['label']
        geo_index = data['dimension']['geo']['category']['index']
        geo_index_map = {v: k for k, v in geo_index.items()}

        time_labels = data['dimension']['time']['category']['label']
        time_index = data['dimension']['time']['category']['index']
        time_index_map = {v: k for k, v in time_index.items()}

        siec_labels = data['dimension']['siec']['category']['label']
        siec_index = data['dimension']['siec']['category']['index']
        siec_index_map = {v: k for k, v in siec_index.items()}

        nrg_labels = data['dimension']['nrg_bal']['category']['label']
        nrg_index = data['dimension']['nrg_bal']['category']['index']
        nrg_index_map = {v: k for k, v in nrg_index.items()}

        unit_labels = data['dimension']['unit']['category']['label']
        unit_index = data['dimension']['unit']['category']['index']
        unit_index_map = {v: k for k, v in unit_index.items()}

        dim_order = data['id']  # ['freq', 'nrg_bal', 'siec', 'unit', 'geo', 'time']
        shape = tuple(data['size'])
        values = data['value']

        records = []

        for flat_index, val in values.items():
            idx = int(flat_index)
            indices = np.unravel_index(idx, shape)

            dim_values = {dim: i for dim, i in zip(dim_order, indices)}

            geo_code = geo_index_map[dim_values['geo']]
            time_code = time_index_map[dim_values['time']]
            siec_code = siec_index_map[dim_values['siec']]
            nrg_code = nrg_index_map[dim_values['nrg_bal']]
            unit_code = unit_index_map[dim_values['unit']]

            record = {
                'Country_Code': geo_code,
                'Country': geo_labels[geo_code],
                'Year': time_labels[time_code],
                'Energy_Product_Code': siec_code,
                'Energy_Product_Label': siec_labels[siec_code],
                'Energy_Balance_Code': nrg_code,
                'Energy_Balance_Label': nrg_labels[nrg_code],
                'Unit': unit_labels[unit_code],
                'Value': val
            }

            records.append(record)

        df = pd.DataFrame(records)
        df = df.sort_values(by=['Country', 'Year', 'Energy_Product_Label']).reset_index(drop=True)

        output_path = "/tmp/eurostat_energy.csv"
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

        # the following is used when running in a local environment, path add the to UI directly
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
        destination_project_dataset_table="data-engineering-411419.raw_eurostat_dev.nrg_bal_s",  # 👈 adjust to your project
       schema_fields=[
            {"name": "Country_Code", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Country", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Year", "type": "STRING", "mode": "REQUIRED"},
            {"name": "Energy_Product_Code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Energy_Product_Label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Energy_Balance_Code", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Energy_Balance_Label", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Unit", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Value", "type": "FLOAT", "mode": "NULLABLE"},
        ],

        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",  # 🔁 Truncates table each time
        gcp_conn_id="google_cloud_default",
    )

    fetch >> upload >> load_to_bq