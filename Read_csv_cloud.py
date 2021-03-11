from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import pandas as pd
import gcsfs #google storage 


# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "/users/marcosmazzarotto/Documents/Boticario/boticario-307001-3b394e68f9d4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#Reading CSVS from google storage
fs = gcsfs.GCSFileSystem(token = credentials, project='boticario-307001')
with fs.open('bucket_boticario/input/Base_2017_1.csv') as f:
    df2017 = pd.read_csv(f,delimiter=';')
    
with fs.open('bucket_boticario/input/Base_2018_2.csv') as f:
    df2018 = pd.read_csv(f,delimiter=';')
    
with fs.open('bucket_boticario/input/Base_2019_3.csv') as f:
    df2019 = pd.read_csv(f,delimiter=';')

#Cleanse Datasets - error when importing due to space in column name
df2017.columns = df2017.columns.str.replace(' ', '') 
df2017.columns = map(str.lower, df2017.columns)

df2018.columns = df2018.columns.str.replace(' ', '') 
df2018.columns = map(str.lower, df2018.columns)

df2019.columns = df2019.columns.str.replace(' ', '') 
df2019.columns = map(str.lower, df2019.columns)

  
#Log-in to BQ
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

#Dataset creation
try:
  dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
except:
    print("Dataset already exists")
else:
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

#Table definition
table_id = client.dataset("boticariodb").table("S_CSV_LOAD")

#start of job load configuration
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE'
    
job = client.load_table_from_dataframe(
    df2017, table_id, job_config=job_config
)  # Make an API reque

job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

#Second and third iterations needs an insert without truncating
job_config.write_disposition = 'WRITE_APPEND'
#table 2018
job = client.load_table_from_dataframe(
    df2018, table_id, job_config=job_config
)  # Make an API reque

# job is async operation so we have to wait for it to finish
job.result()
#validation
table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

#table 2019
job = client.load_table_from_dataframe(
    df2019, table_id, job_config=job_config
)  # Make an API reque

# job is async operation so we have to wait for it to finish
job.result()
#validation
table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

#REMOVING DUPLICATED RECORDS
print("Removing duplicated records and re-inserting the data")
df = client.query(
    """
    SELECT *
    FROM (
      SELECT
          *,
          ROW_NUMBER()
              OVER (PARTITION BY id_marca, id_linha, data_venda, qtd_venda)
              row_number
      FROM boticariodb.S_CSV_LOAD
    )
    WHERE row_number = 1
                   
    """
).to_dataframe()

job_config = bigquery.LoadJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE'
    
job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)  # Make an API reque

#Insert the data without duplicates
try:
  job.result()  # Waits for the query to finish.
except:
    print("Error inserting the data")
else:
  print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

