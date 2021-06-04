from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

BUCKET_PATH = Variable.get("BUCKET_PATH")
BUCKET_INPUT = Variable.get("BUCKET_INPUT")
PROJECT_ID = Variable.get("PROJECT_ID")

convert_search_keyword_query = f"""
    SELECT
        SAFE_CAST(user_id AS INT64) user_id,
        search_keyword,
        SAFE_CAST(search_result_count AS INT64) search_result_count,
        created_at
    FROM
        `{PROJECT_ID}.search_history.temp_search_history`
"""

ds = "{{ ds }}"
most_searched_keyword_query = f"""
    SELECT
        search_keyword,
        SUM(search_result_count) AS total_search_result,
        '{ds}' AS date
    FROM
        `{PROJECT_ID}.search_history.converted_search_history`
    WHERE
        SAFE_CAST(LEFT(created_at, 10) AS DATE) = '{ds}'
    GROUP BY
        search_keyword
    ORDER BY
        SUM(search_result_count) DESC
    LIMIT 1;
"""

default_args = {
    "start_date": datetime(2021, 3, 10),
    "end_date": datetime(2021, 3, 15),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": PROJECT_ID,
        "temp_location": BUCKET_PATH + "/tmp/",
        "numWorkers": 1,
    },
}

with DAG(
    'daily_search_dag',
    default_args=default_args,
    description='ETL Blank Space Week 2',
    schedule_interval=timedelta(days=1),
    tags=['blank_space', 'bigquery', 'dataflow'],
) as dag:

    gcs_to_bq = GoogleCloudStorageToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=BUCKET_INPUT,
        source_objects=["keyword_search_search_{{ ds_nodash }}.csv"],
        destination_project_dataset_table="search_history.temp_search_history",
        source_format="csv",
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'user_id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'search_keyword', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'search_result_count', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'created_at', 'type': 'STRING', 'mode': 'REQUIRED'},
        ],
        schema_object=BUCKET_PATH + '/json_schema.json',
        write_disposition="WRITE_TRUNCATE",
        wait_for_downstream=True,
        depends_on_past=True
    )

    convert_data_type = BigQueryOperator(
        task_id='convert_data_type',
        sql=convert_search_keyword_query,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=PROJECT_ID +
        ":search_history.converted_search_history",
        use_legacy_sql=False,
    )

    bq_top_search_keywords = BigQueryOperator(
        task_id='bq_top_search_keywords',
        sql=most_searched_keyword_query,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=PROJECT_ID +
        ":search_history.top_search_history",
        use_legacy_sql=False,
    )

    gcs_to_bq >> convert_data_type >> bq_top_search_keywords
