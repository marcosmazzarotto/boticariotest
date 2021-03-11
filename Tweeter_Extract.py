#Libraries
import re as re
import tweepy as tw
from datetime import datetime
from textblob import TextBlob
import pandas as pd

from google.cloud import bigquery
from google.oauth2 import service_account

from google.cloud import storage
import os

##############################################################################################################################
#Tweeter extract Step
CONSUMER_KEY = ''
CONSUMER_SECRET = ''
ACCESS_TOKEN = ''
ACCESS_TOKEN_SECRET = ''

#BIGQUERY Credentials
key_path = "/users/marcosmazzarotto/Documents/Boticario/boticario-307001-3b394e68f9d4.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

#Checking the data for linhas in Dec/2019
sql = """
     select  linha
            
    from (
        SELECT 
            linha,
            RANK() OVER(ORDER BY total_vendas DESC) AS rank_Id

        FROM
            boticariodb.AGG_VENDAS_LINHA_ANO_MES
        WHERE ANO = 2019
        AND MES = 12
    ) as A
    where rank_id = 1
"""

df = client.query(sql).to_dataframe()

#remove index from string
sqloutput = df.iloc[-1,:].to_string(header=False, index=False)
#remove spaces
sqloutput = sqloutput.replace(" ", "")

#Parameters from exercise
keyword = ("'Boticário' OR '{}'").format(sqloutput)
count = 50
lang = 'pt'

class TweetAnalyzer():

  def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
    '''
      Conectar com o tweepy
    '''
    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    self.conToken = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, retry_count=5, retry_delay=10)

  def __clean_tweet(self, tweets_text):
    '''
    Tweet cleansing.
    '''

    clean_text = re.sub(r'RT+', '', tweets_text) 
    clean_text = re.sub(r'@\S+', '', clean_text)  
    clean_text = re.sub(r'https?\S+', '', clean_text) 
    clean_text = clean_text.replace("\n", " ")

    return clean_text

  def search_by_keyword(self, keyword, count=count, result_type='mixed', lang=lang, tweet_mode='extended'):
    '''
      Search for the twitters that has commented the keyword subject.
    '''
    tweets_iter = tw.Cursor(self.conToken.search,
                          q=keyword, tweet_mode=tweet_mode,
                          rpp=count, result_type=result_type,
                          lang=lang, include_entities=True).items(count)

    return tweets_iter

  def prepare_tweets_list(self, tweets_iter):
    '''
      Transforming the data to DataFrame.
    '''

    tweets_data_list = []
    for tweet in tweets_iter:
      if not 'retweeted_status' in dir(tweet):
        tweet_text = self.__clean_tweet(tweet.full_text)
        tweets_data = {
            'len' : len(tweet_text),
            'ID' : tweet.id,
            'User' : tweet.user.screen_name,
            'UserName' : tweet.user.name,
            'UserLocation' : tweet.user.location,
            'TweetText' : tweet_text,
            'Language' : tweet.user.lang,
            'Date' : tweet.created_at,
            'Source': tweet.source,
            'Likes' : tweet.favorite_count,
            'Retweets' : tweet.retweet_count,
            'Coordinates' : tweet.coordinates,
            'Place' : tweet.place 
        }

        tweets_data_list.append(tweets_data)

    return tweets_data_list


analyzer = TweetAnalyzer(consumer_key = CONSUMER_KEY, consumer_secret = CONSUMER_SECRET, 
access_token = ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET)

# Para realizar as buscas usando a keyword e quantidade predefinida e converter em uma lista.
tweets_iter = analyzer.search_by_keyword(keyword, count)
tweets_list = analyzer.prepare_tweets_list(tweets_iter)

#Moving data to Dataframe
tweets_df = pd.DataFrame(tweets_list)
#selecting 2 columns
tweets_df = tweets_df[['UserName', 'TweetText']]

#define key to connect to storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

client = storage.Client()
bucket = client.get_bucket('bucket_boticario')

try:
    bucket.blob('output/tweets.csv').upload_from_string(tweets_df.to_csv(index=False), 'text/csv')
except:
    print("Error creating the csv file")
else:
    print("File created successfully")    






'''''
############################################################################################################
#Libraries
from google.cloud import bigquery
from google.oauth2 import service_account

#BigQuery Step
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

job = client.load_table_from_dataframe(
    tweets_df, table_id, job_config=job_config
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
'''''

