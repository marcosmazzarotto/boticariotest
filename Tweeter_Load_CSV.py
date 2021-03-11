############################################################################################################
#Libraries
from google.cloud import bigquery
from google.oauth2 import service_account

import pandas as pd
import gcsfs #google storage 

#Authentication
key_path = "/users/marcosmazzarotto/Documents/Boticario/boticario-307001-3b394e68f9d4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#creating instance
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

table_id = client.dataset("boticariodb").table("Tweets_Extract")
#start of job load configuration
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("UserName", "STRING"),
        bigquery.SchemaField("TweetText", "STRING"),
    ],
    write_disposition="WRITE_TRUNCATE",
)

#Reading CSVS from google storage
fs = gcsfs.GCSFileSystem(token = credentials, project='boticario-307001')


with fs.open('bucket_boticario/output/tweets.csv') as source_file:
    job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )  # Make an API request.

job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)


