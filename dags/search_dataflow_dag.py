from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.dummy import DummyOperator

BUCKET_PATH = Variable.get("BUCKET_PATH")
BUCKET_INPUT = Variable.get("BUCKET_INPUT")
PROJECT_ID = Variable.get("PROJECT_ID")
GCE_REGION = Variable.get("GCE_REGION") 
GCE_ZONE = Variable.get("GCE_ZONE") 
OUTPUT_FULL_TABLE = Variable.get("ALL_KEYWORDS_BQ_OUTPUT_TABLE")
OUTPUT_FINAL_TABLE = Variable.get("TOP_KEYWORDS_BQ_OUTPUT_TABLE")

ds = "{{ ds }}"
top_result_query = f"""
    SELECT
        lower(search_keyword) as keyword,
        count(lower(search_keyword)) as search_count,
        created_at as date
    FROM
        `{PROJECT_ID}.{OUTPUT_FULL_TABLE}`
    WHERE
        created_at = '{ds}'
    GROUP BY
        keyword, created_at
    ORDER BY
        search_count desc
    LIMIT 1;
"""

default_args = {
    "start_date": datetime(2021,3,10),
    "end_date"  : datetime(2021,3,15), 
    "dataflow_default_options": {
        "project": PROJECT_ID,  
        "region": GCE_REGION, 
        "zone": GCE_ZONE,
        "temp_location": BUCKET_PATH + "/tmp/",
        "numWorkers": 1,
    },
}

with DAG(
    "search_dataflow_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
) as dag:
    
    gcs_to_bq = DataflowTemplateOperator(
        task_id="gcs_to_bq",
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        parameters={
            "javascriptTextTransformFunctionName": "transform_csv_to_json",
            "JSONPath": BUCKET_PATH + "/json_schema.json",
            "javascriptTextTransformGcsPath": BUCKET_PATH + "/transform.js",
            "inputFilePattern": BUCKET_INPUT + "/keyword_search_search_{{ ds_nodash }}.csv",
            "outputTable": OUTPUT_FULL_TABLE,
            "bigQueryLoadingTemporaryDirectory": BUCKET_PATH + "/tmp/",
        },
    )  

    bq_top_search = BigQueryOperator(
        task_id = "bq_top_search",
        sql = top_result_query,
        use_legacy_sql = False,
        destination_dataset_table = OUTPUT_FINAL_TABLE,
        write_disposition = 'WRITE_APPEND'
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> gcs_to_bq >> bq_top_search >> end