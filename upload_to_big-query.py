import pandas as pd
# import pandas_gbq
from google.cloud import bigquery

GOOGLE_APPLICATION_CREDENTIALS = "secrets/encoded-axis-369815-759553ed797a.json"
client = bigquery.Client.from_service_account_json(
    GOOGLE_APPLICATION_CREDENTIALS)


table_id = 'encoded-axis-369815.SCRAPED_IRISH_ELECTIONS.dail_elections'

#table = bigquery.Table(table_id)
# table = client.create_table(table)  # Make an API request.
#print(f"Created table {table.project} {table.dataset_id} {table.table_id}")

DF = pd.read_parquet('DAIL_elections_master.parquet')
print(DF)

job = client.load_table_from_dataframe(DF, table_id)
job.result()
