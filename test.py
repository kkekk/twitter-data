import os
import tweepy
# consumer_token =
# consumer_secret =
# access_token =
# access_token_secret =

auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)
class MyStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        print(data)
        return True

    def on_error(self, status):
        print(status)

myStreamListener = MyStreamListener()
try:
    stream = tweepy.Stream(auth=api.auth,
                           listener=myStreamListener)
    print("listener starting...")
    stream.filter(track=['python'])
except Exception as e:
    print(e)
    print(e.__doc__)
