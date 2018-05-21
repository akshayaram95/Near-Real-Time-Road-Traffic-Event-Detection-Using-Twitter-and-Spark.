from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import json
import re

access_token ='YOUR ACCESS TOKEN'
access_token_secret = 'YOUR SECRET ACCESS TOKEN'
consumer_key =  'YOUR CONSUMER KEY'
consumer_secret =  'YOUR SECRET CONSUMER KEY'

hashtags=["traffic nyc","#traffic #nyc", "#traffic #ny", "#traffic #newyorkcity", "#traffic #newyork", "#accident #newyorkcity",
          "#roadblock #newyorkcity", "#accident #newyork","#roadblock #newyork", "#accident #nyc",
          "#roadblock #nyc", "#accident #ny","#roadblock #ny"]
#hashtags=["#traffic #cleaveland","#traffic #cleaveland", "#traffic #cleaveland", "#traffic #accident #cleaveland","#traffic #roadblock #cleaveland"]
#hashtags=["#traffic", "#traffic #accident","#traffic #roadblock"]

class StdOutListener(StreamListener):
	def on_data(self, data):
		status=json.loads(data)
		try:
			processed_String = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\ART )", " ", status['text']).split())
			processed_String = ' '.join(processed_String.split('\n'))
			print(status['text'])
			print(status['user']['location'])
			print(status['created_at'])
			print(processed_String)
			new_data={'text':processed_String,'created_at':status['created_at'],'geo':status['geo'],'coordinates':status['coordinates']}#,'location':status['user']['location']
			new_data=json.dumps(new_data)
			producer.send_messages("trafficnyc", new_data.encode('utf-8'))
			print('--------------------------------------------------')
			return True
		except:
			print('----------------------------------------------------')

	def on_error(self, status):
		print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="traffic",languages=["en"],follow=['42640432'])#,locations=[-74.1687,40.5722,-73.8062,40.9467])