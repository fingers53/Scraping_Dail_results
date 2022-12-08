import pandas as pd
from google.cloud import bigquery

GOOGLE_APPLICATION_CREDENTIALS = "secrets/encoded-axis-369815-759553ed797a.json"
client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS)


table_id = 'encoded-axis-369815.SCRAPED_IRISH_ELECTIONS.All_Elections_Every_Candidate'

#table = bigquery.Table(table_id)
# table = client.create_table(table)  # Make an API request.
#print(f"Created table {table.project} {table.dataset_id} {table.table_id}")

DF = pd.read_parquet('ALL_CANDIDATES.parquet')
print(DF)

job = client.load_table_from_dataframe(DF, table_id)
print(job.result())
