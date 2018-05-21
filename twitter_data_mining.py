import json
import tweepy
from elasticsearch import Elasticsearch
import requests
import re

ACCESS_TOKEN = '982440453936242688-cxHysxbD2RAxuamp7GLv2O1uaW75hlb'
ACCESS_SECRET = '11wheFPpJSsHbAuh0veSBKI7Bz6t06mZvewaEjgUii3Tn'
CONSUMER_KEY = '6sRDHYQ5gPPmSspaqhnPgzErS'
CONSUMER_SECRET = '1kLAwSt9YCUlovHgEOgo0EOvkIflxM3E3tMmnaS5KCLhSahLyr'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)
api = tweepy.API(auth)

headers = {'content-type': 'application/json'}
elasticsearch_index_uri = 'http://elastic:KWKWmZTobKtc3WsjVwWB@localhost:9200/twitter_data_mining/tweet'
mapping = {
    "mappings": {
        "tweet": {
            "properties": {
                "text": {
                    "type": "keyword"
                },
                "timestamp": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss"
                },
            }
        }
     }
}
es = Elasticsearch(http_auth=('elastic', 'KWKWmZTobKtc3WsjVwWB'))
es.indices.delete(index='twitter_data_mining')
es.indices.create(index='twitter_data_mining', body=mapping, ignore=400)

for tweet in tweepy.Cursor(api.search,
                           q=["traffic nyc"],
                           rpp=100,
                           result_type="recent",
                           include_entities=True,
                           lang="en").items():
    print(tweet.text)
    processed_String = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\ART )", "", tweet.text).split())
    processed_String = ' '.join(processed_String.split('\n'))
    doc = {
        'text': processed_String,
        'timestamp': tweet.created_at.strftime("%Y-%m-%d %H:%M:%S")
    }
    print(doc)
    requests.post(elasticsearch_index_uri, data=json.dumps(doc), headers=headers)
