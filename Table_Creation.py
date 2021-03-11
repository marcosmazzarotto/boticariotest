from google.cloud import bigquery
from google.oauth2 import service_account

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "/users/marcosmazzarotto/Documents/Boticario/boticario-307001-3b394e68f9d4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


# TODO(developer): Set dataset_id to the ID of the dataset to create.
dataset_id = "{}.boticariodb".format(client.project)

# Construct a full Dataset object to send to the API.
dataset = bigquery.Dataset(dataset_id)

# TODO(developer): Specify the geographic location where the dataset should reside.
dataset.location = "US"

# Send the dataset to the API for creation, with an explicit timeout.
# Raises google.api_core.exceptions.Conflict if the Dataset already
# exists within the project

try:
  dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
except:
    print("Dataset already exists")
else:
  print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

#DDL Step
sql = """    
    CREATE OR REPLACE TABLE
    `{}.{}.AGG_VENDAS_MES_ANO` AS (
  SELECT
    EXTRACT (YEAR FROM DATA_VENDA) AS ANO,
    EXTRACT (MONTH FROM DATA_VENDA) AS MES,
    COUNT(qtd_venda) AS TOTAL_VENDAS 
  FROM
    `{}.{}.S_CSV_LOAD`
  GROUP BY
    1,2 
    )          
    """.format(
    client.project, dataset.dataset_id,client.project, dataset.dataset_id
)
    
job = client.query(sql)  # API request.


try:
  job.result()  # Waits for the query to finish.
except:
    print("Table already exists")
else:
  print("Created table {}.{}".format(client.project, dataset.dataset_id))
    
    
#DDL Step
sql = """    
    CREATE OR REPLACE TABLE
    `{}.{}.AGG_VENDAS_MARCA_LINHA` AS (
  SELECT
     marca,
    linha,
    COUNT(qtd_venda) AS TOTAL_VENDAS 
  FROM
    `{}.{}.S_CSV_LOAD`
  GROUP BY
    1,2 
    )          
    """.format(
    client.project, dataset.dataset_id,client.project, dataset.dataset_id
)
    
job = client.query(sql)  # API request.


try:
  job.result()  # Waits for the query to finish.
except:
    print("Table already exists")
else:
  print("Created table {}.{}".format(client.project, dataset.dataset_id))

    
sql = """    
    CREATE OR REPLACE TABLE
    `{}.{}.AGG_VENDAS_MARCA_ANO_MES` AS (
  SELECT
      marca,
    EXTRACT (YEAR FROM DATA_VENDA) AS ANO,
    EXTRACT (MONTH FROM DATA_VENDA) AS MES,
    COUNT(qtd_venda) AS TOTAL_VENDAS 
  FROM
    `{}.{}.S_CSV_LOAD`
  GROUP BY
    1,2,3 
    )          
    """.format(
    client.project, dataset.dataset_id,client.project, dataset.dataset_id
)
    
job = client.query(sql)  # API request.


try:
  job.result()  # Waits for the query to finish.
except:
    print("Table already exists")
else:
  print("Created table {}.{}".format(client.project, dataset.dataset_id))
    

sql = """    
    CREATE OR REPLACE TABLE
    `{}.{}.AGG_VENDAS_LINHA_ANO_MES` AS (
  SELECT
      LINHA,
    EXTRACT (YEAR FROM DATA_VENDA) AS ANO,
    EXTRACT (MONTH FROM DATA_VENDA) AS MES,
    COUNT(qtd_venda) AS TOTAL_VENDAS 
  FROM
    `{}.{}.S_CSV_LOAD`
  GROUP BY
    1,2,3 
    )          
    """.format(
    client.project, dataset.dataset_id,client.project, dataset.dataset_id
)
    
job = client.query(sql)  # API request.


try:
  job.result()  # Waits for the query to finish.
except:
    print("Table already exists")
else:
  print("Created table {}.{}".format(client.project, dataset.dataset_id))
