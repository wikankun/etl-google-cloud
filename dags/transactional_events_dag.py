from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

PROJECT_ID = Variable.get("PROJECT_ID")
SOURCE_PROJECT_ID = Variable.get("SOURCE_PROJECT_ID")
KEY_FILE = '/home/airflow/gcs/data/keyfile.json'


def value(col):
    int_value = ['transaction_id', 'transaction_detail_id',
                 'purchase_quantity']
    str_value = ['transaction_number', 'purchase_payment_method',
                 'purchase_source', 'product_id']
    float_value = ['purchase_amount']
    if col in int_value:
        return 'value.int_value'
    elif col in str_value:
        return 'value.string_value'
    return 'value.float_value'

def integrate_transaction(**kwargs):
    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE
    )
    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )
    sql = f"""SELECT *
            FROM {PROJECT_ID}.transactions.event
            WHERE (event_name = 'purchase_item')
            AND (event_datetime between '{kwargs.get('prev_ds')}'
            AND '{kwargs.get('ds')}')
    """
    query = client.query(sql)
    results = query.result()
    df = results.to_dataframe()

    # transform data from table result
    columns = [
        'transaction_id',
        'transaction_detail_id',
        'transaction_number',
        'purchase_quantity',
        'purchase_amount',
        'purchase_payment_method',
        'purchase_source',
        'transaction_datetime',
        'product_id',
        'user_id',
        'state',
        'city',
        'created_at',
        'ext_created_at']
    entity = []
    for idx in range(len(df)):
        temp_row_entity = {}
        row = df.iloc[idx]
        event_params = pd.json_normalize(row['event_params']).set_index('key')
        for key in columns[:7]:
            try:
                temp_row_entity[key] = event_params[value(key)][key]
            except BaseException:
                temp_row_entity[key] = None
        temp_row_entity['transaction_datetime'] = row['event_datetime']
        temp_row_entity['user_id'] = row['user_id']
        temp_row_entity['state'] = row['state']
        temp_row_entity['city'] = row['city']
        temp_row_entity['created_at'] = row['created_at']
        temp_row_entity['ext_created_at'] = kwargs.get('ds')
        entity.append(temp_row_entity)

    # push data to your own bigquery
    credentials = service_account.Credentials.from_service_account_file(
        KEY_FILE)
    pd.DataFrame(entity).to_gbq(
        destination_table="transactions.transaction",
        project_id=PROJECT_ID,
        if_exists="replace",
        credentials=credentials
    )


default_args = {
    "start_date": datetime(2021, 3, 21),
    "end_date": datetime(2021, 3, 29),
    "depends_on_past": True,
}

with DAG(
    "transactional_events_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=3),
) as dag:

    copy_bq_table = PythonOperator(
        task_id='copy_bq_table',
        python_callable=copy_table,
        provide_context=True
    )

    transactional_events = PythonOperator(
        task_id='etl_to_bq',
        python_callable=integrate_transaction,
        provide_context=True
    )

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    start >> transactional_events >> end
