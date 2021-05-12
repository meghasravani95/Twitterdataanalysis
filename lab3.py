# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 19:36:34 2021

@author: srava
"""

from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import pymongo
from pymongo import MongoClient
import json


#consumer key, consumer secret, access token, access secret.
CONSUMER_KEY= "gun1fifd8iaKXLAeiFJ8kRr34"
CONSUMER_SECRET="rIpsyP6IM6DFW6O5IJGD9deyRaOLuZJjX8gALqX3rZkTOyF5LL"
OAUTH_TOKEN="1371821070681059331-C1OauRpgy5kT7MjyMU1O7bjehdhBYq"
OAUTH_TOKEN_SECRET="sAEzpDbVvwLtmFes8htHC8HVoMRWM7KKHRxMZfIx9A7oO"
# The MongoDB connection info.
conn = MongoClient('localhost', 27017)
# This assumes your database name is ElectionDataStream.
db = conn.CovidStream
# Your collection name is tweets.
collection = db.tweets
db.tweets.create_index([("id", pymongo.ASCENDING)],unique = True,)
class getStreamData(StreamListener):
    def on_data(self, data):
 # Load the Tweet into the variable "tweet"
         try:
            tweet = json.loads(data)
 # Pull important data from the tweet to store in the database
 #One at a time.
            collection.insert_one(tweet)
            return True
         except:
             pass
        

if __name__ == "__main__":
    authentication = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    authentication.set_access_token(OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    tweetStream = Stream(authentication, getStreamData())
 # Here write down your keywords which you want to search for.
    tweetStream.filter(track=["Coronavirus","Covid","Covid 19"])