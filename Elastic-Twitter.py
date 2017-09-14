#Making necessary Imports
import tweepy
import sys  
import json  
from tweepy import OAuthHandler
from textwrap import TextWrapper  
from elasticsearch import Elasticsearch


#Connecting to Twitter via OAuth2

consumer_key = '#########################'
consumer_secret = '##################################################'
access_token = '##################################################'
access_secret = '#############################################'
 
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)
 
api = tweepy.API(auth)


# Connecting to Elasticsearch
es = Elasticsearch([{'host' : '192.168.1.13', 'port' : 9200}])

# Creating ES Index
es.indices.create(index='twitter_index', ignore=400)


# Create StreamApi Class to get data from twitter stream and save it in ES Index created above

class StreamApi(tweepy.StreamListener):  
    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')

    def on_status(self, status):
        
            #print 'n%s %s' % (status.author.screen_name, status.created_at)

            json_data = status._json
#             print json_data['text']

            es.index(index="twitter_index",
                      doc_type="twitter",
                      body=json_data,
                      ignore=400
                     )


# Start streaming

streamer = tweepy.Stream(auth=auth, listener=StreamApi(), timeout=60)

#Fill with your own Keywords bellow
terms = ['data science', 'python', '#datascience']

streamer.filter(None,terms)  
