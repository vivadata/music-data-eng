from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

PROJECT_ID = "advance-vector-468708-t0"
DATASET = "batch67_sql"
BUCKET_NAME = "monbucketoliviergoz"

default_args = {"start_date": datetime(2025, 1, 1)}

with DAG(
    "gcs_to_bq_view",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:

    # 1️⃣ Charger les fichiers CSV depuis GCS vers BigQuery
    load_products = GCSToBigQueryOperator(
        task_id="load_products",
        bucket=BUCKET_NAME,
        source_objects=["data/products.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.products_staging",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        autodetect=True,
    )

    load_departments = GCSToBigQueryOperator(
        task_id="load_departments",
        bucket=BUCKET_NAME,
        source_objects=["data/departements.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.departments_staging",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        autodetect=True,
    )

    # 2️⃣ Créer la view avec la jointure
    create_view = BigQueryInsertJobOperator(
        task_id="create_products_with_department_view",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET}.products_with_department` AS
                    SELECT
                        p.*,
                        d.department_name
                    FROM
                        `{PROJECT_ID}.{DATASET}.products_staging` p
                    LEFT JOIN
                        `{PROJECT_ID}.{DATASET}.departments_staging` d
                    ON
                        p.department_id = d.department_id
                """,
                "useLegacySql": False,
            }
        },
    )

    # Définir l'ordre d'exécution
    [load_products, load_departments] >> create_view

