from google.cloud import bigquery
from google.api_core.exceptions import Conflict
from config import client_prod

schemaTempKeyword = [
    bigquery.SchemaField("user_id",
                         "STRING",
                         mode="REQUIRED"),

    bigquery.SchemaField("search_keyword",
                         "STRING",
                         mode="REQUIRED"),

    bigquery.SchemaField("search_result_count",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("created_at",
                         "STRING",
                         mode="REQUIRED")
]

schemaConvertedKeyword = [
    bigquery.SchemaField("user_id",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("search_keyword",
                         "STRING",
                         mode="REQUIRED"),

    bigquery.SchemaField("search_result_count",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("created_at",
                         "STRING",
                         mode="REQUIRED")
]

schemaTopKeyword = [
    bigquery.SchemaField("search_keyword",
                         "STRING",
                         mode="REQUIRED"),

    bigquery.SchemaField("total_search_result",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("date",
                         "STRING",
                         mode="REQUIRED")
]

schemaTransaction = [
    bigquery.SchemaField("transaction_id",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("transaction_detail_id",
                         "INTEGER"),

    bigquery.SchemaField("transaction_number",
                         "STRING"),

    bigquery.SchemaField("transaction_datetime",
                         "STRING"),

    bigquery.SchemaField("purchase_quantity",
                         "INTEGER"),

    bigquery.SchemaField("purchase_amount",
                         "FLOAT64"),

    bigquery.SchemaField("purchase_payment_method",
                         "STRING"),

    bigquery.SchemaField("purchase_source",
                         "STRING"),

    bigquery.SchemaField("product_id",
                         "INTEGER"),

    bigquery.SchemaField("user_id",
                         "INTEGER",
                         mode="REQUIRED"),

    bigquery.SchemaField("state",
                         "STRING"),

    bigquery.SchemaField("city",
                         "STRING"),

    bigquery.SchemaField("created_at",
                         "STRING",
                         mode="REQUIRED"),

    bigquery.SchemaField("ext_created_at",
                         "STRING",
                         mode="REQUIRED"),
]


def create_table(table_id, schema):
    try:
        table = bigquery.Table(table_id, schema=schema)
        # Make an API request
        table = client_prod.create_table(table)
        print(f"Table {table_id} Created!")
    except Conflict:
        print(f"Table {table_id} Already Exist!")
