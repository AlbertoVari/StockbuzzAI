import os
import time
import tweepy
import pandas as pd
import json
import argparse
from textnorm import normalize_space
from tabulate import tabulate
from tweepy.auth import OAuthHandler
from google.cloud import language
from google.cloud import bigquery

## Authenticate to Twitter
consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)


# Create API object
api = tweepy.API(auth)
api = tweepy.API(auth, wait_on_rate_limit=True)

# Search SETUP
number_tweet =  1000
date_since = "2021-2-1"
search_words = "$SLV"
new_search = search_words + " -filter:retweets"
tweets = tweepy.Cursor(api.search,
                       q=new_search,
                       lang="en",
                       since=date_since).items(number_tweet)

users_locs = [[tweet.user.screen_name, tweet.user.location, tweet.text] for tweet in tweets]

tweet_text = pd.DataFrame(data=users_locs,columns=['User', "Location","Text"])

table  = pd.DataFrame.insert(tweet_text, loc=3, column='Score', value="")

table  = pd.DataFrame.insert(tweet_text, loc=4, column='Magnit', value="")

table  = pd.DataFrame.insert(tweet_text, loc=5, column='Date', value="")

tweet_text.to_csv(r'silverred.csv')

file_tweet  = 'silverred.csv'

client = language.LanguageServiceClient()

df = pd.DataFrame(tweet_text) 

table_id =''

i = 0


while  i < number_tweet:
        time.sleep(1)
        client = language.LanguageServiceClient() 
        clientquery  = bigquery.Client()

        content =  str(df.at[i, 'Text'])
        user = str(df.at[i, 'User'])
        location = str(df.at[i, 'Location'])
        date = str(df.at[i, 'Date'])

        content = normalize_space(content)
        location = normalize_space(location)

        content = content.replace("'","-")
        content = content.replace(",","-")
 
        location = location.replace("'","-")
        location = location.replace(",","-")

        location.replace(" ","_")
        location.splitlines()

        document = language.Document(content=content, type_=language.Document.Type.PLAIN_TEXT)
        response  = client.analyze_sentiment(request={'document': document})

        score_tweet = format(response.document_sentiment.score)
        magnitude_tweet = format(response.document_sentiment.magnitude)

        df.at[i, 'Score'] = score_tweet
        df.at[i, 'Magnit'] = magnitude_tweet
        content = content[:50]


        sql = "INSERT  " + table_id + " (number,user,location,text,magnit,score) " + \
               "VALUES(" + str(i) + ",'" +str(user) + "','" + str(location) + "','" + str(content) + \
                "'," + str(magnitude_tweet) + "," + str(score_tweet) + ")"

        sql = str(sql)

        query_job = clientquery.query(sql)
        results = query_job.result()
        print(i,user,location,content[:20],score_tweet)

        i += 1

tweet_text.to_csv(r'silverred.csv')
# print(tabulate(df, headers='keys', tablefmt='psql'))





