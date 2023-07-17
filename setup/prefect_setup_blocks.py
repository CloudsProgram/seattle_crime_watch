from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.bigquery import BigQueryWarehouse
import os
import json
from dotenv import load_dotenv

load_dotenv()

gcp_account_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], 'r') as creds:
    json_creds = json.load(creds)
    gcp_project_name = json_creds['project_id']



credentials_block = GcpCredentials(
    service_account_file=f"{gcp_account_file}"  # point to your credentials .json file
)
credentials_block.save("seattle-crime-cred", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("seattle-crime-cred"),
    bucket=f"seattle_crime_data_lake_{gcp_project_name}",  # insert your GCS bucket name
)

bucket_block.save("seattle-crime-gcs", overwrite=True)

bq_block = BigQueryWarehouse(
    gcp_credentials=GcpCredentials.load("seattle-crime-cred"),
    fetch_size = 1
)

bq_block.save("seattle-crime-bq", overwrite=True)