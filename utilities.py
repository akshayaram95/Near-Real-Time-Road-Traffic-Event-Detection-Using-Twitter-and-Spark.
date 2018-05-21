from elasticsearch import Elasticsearch
from streetaddress import StreetAddressParser
import requests
import requests
import json

addr_parser = StreetAddressParser()
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
                "location": {
                    "type": "geo_point"
                },
            }
        }
     }
}
es = Elasticsearch(http_auth=('elastic', 'KWKWmZTobKtc3WsjVwWB'))
es.indices.create(index='twitter_traffic_nyc', body=mapping, ignore=400)

def index_to_elasticsearch(x):
    print('=================================================================================')
    json_data=json.loads(x)
    doc = {
        'text': json_data['text'],
        'location': json_data['location'],
        'timestamp': json_data['created_at']
    }
    requests.post(elasticsearch_index_uri, data=json.dumps(doc), headers=headers)

def get_google_results(txt):
    address = addr_parser.parse(txt)
    geocode_url = "https://maps.googleapis.com/maps/api/geocode/json?address={}".format(address)
    results = requests.get(geocode_url)
    results = results.json()
    if len(results['results']) == 0:
        output = {
            "latitude": None,
            "longitude": None
        }
    else:
        answer = results['results'][0]
        output = {
            "latitude": answer.get('geometry').get('location').get('lat'),
            "longitude": answer.get('geometry').get('location').get('lng')
        }

    return output
