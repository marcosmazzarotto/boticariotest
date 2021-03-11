from google.cloud import bigquery
from google.oauth2 import service_account

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "/users/marcosmazzarotto/Documents/Boticario/boticario-307001-3b394e68f9d4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

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

'''''
#Creating dataset for tables
# Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

# TODO(developer): Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.boticariodb".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project.
dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

#deleting table before inserting
table = bigquery.Table(table_id, schema=schema)
table = client.delete_table(table)  # Make an API request.

dataset = bigquery.

#Schema definition
schema = [
        bigquery.SchemaField("id_marca", "INT64"),
        bigquery.SchemaField("marca", "STRING"),
        bigquery.SchemaField("id_linha", "INT64"),
        bigquery.SchemaField("linha", "STRING"),
        bigquery.SchemaField("data_venda", "date"),
        bigquery.SchemaField("qtd_venda", "INT64")
]

#table creation
table = bigquery.Table(table_ref2017, schema=schema)
table = client.create_table(table)  # Make an API request.
print(
    "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
)
'''''

#start of job load configuration
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1  # ignore the header
job_config.autodetect = True
job_config.field_delimiter = ';' #delimiter
job_config.write_disposition = 'WRITE_TRUNCATE'


with open("Base_2017_1.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )

# job is async operation so we have to wait for it to finish
job.result()
#validation
table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)

#Second and third iterations needs an insert without truncating
job_config.write_disposition = 'WRITE_APPEND'
#table 2018
with open("Base_2018_2.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )

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
with open("Base_2019_3.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )

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

job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


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

