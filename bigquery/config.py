from google.cloud import bigquery
from google.oauth2 import service_account

KEY_FILE = 'keyfile.json'
PROJECT_ID = 'blank-space-315611'
CREDENTIAL_PROD = service_account.Credentials.from_service_account_file(
    KEY_FILE
)
client_prod = bigquery.Client(
    credentials=CREDENTIAL_PROD,
    project=PROJECT_ID
)
