import pandas as pd
from google.cloud import bigquery

GOOGLE_APPLICATION_CREDENTIALS = "secrets/encoded-axis-369815-759553ed797a.json"

GOOGLE_APPLICATION_CREDENTIALS = 'secrets/big_key.json'

client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS)


table_id = 'red-bus-371614.Testing.All_Elections_Every_Candidate'

table = bigquery.Table(table_id)
table = client.create_table(table)  # Make an API request.
print(f"Created table {table.project} {table.dataset_id} {table.table_id}")

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)


DF = pd.read_parquet('irelandelection/ALL_CANDIDATES.parquet')
print(DF)

job = client.load_table_from_dataframe(DF, table_id, job_config=job_config)
print(job.result())
