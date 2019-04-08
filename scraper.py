import os
import tweepy
from dotenv import load_dotenv
from py2neo import Graph, Node, Relationship
from retrying import retry
from pprint import pprint
from objects import Tweet, User

load_dotenv()

consumer_token = os.getenv('CONSUMER_TOKEN')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')

class MyStreamListener(tweepy.StreamListener):
    def __init__(self):
        super().__init__()
        self.count = 1
        graph.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE")
        graph.run("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE")

    def on_status(self, tweet):
        print("==================COUNT: {}==================".format(self.count))
        print(tweet.text)
        try:
            self.run_transaction(tweet)
            self.count += 1
        except Exception as e:
            print("ERROR: ",e)


    def on_error(self, status_code):
        print("Error code: ", status_code)

    @retry(wait_fixed=5000)
    def run_stream(self,auth):
        stream = tweepy.Stream(auth=auth, listener=self, timeout=5, retry_420_start=5)
        try:
            print("listener starting...")
            stream.filter(locations=[-130.56,23.59,-77.09,48.77])
        except Exception as e:
            print(e)
            print(e.__doc__)


    def run_transaction(self, t):
        t = t._json
        user = self.user_node(t['user'])
        tweet = self.tweet_node(t)
        if t.get('in_reply_to_status_id'):
            replied = self.replied_node(t)
            tweet.replied_to.add(replied)

        user.tweeted.add(tweet)
        graph.push(user)

        if t.get('retweeted_status') or t.get('quoted_status'):
            t2 = t.get('retweeted_status') or t.get('quoted_status')
            retweet = self.tweet_node(t2)
            if t2.get('in_reply_to_status_id'):
                replied2 = self.replied_node(t2)
                tweet.replied_to.add(replied2)


            tweet.retweeted.add(retweet)
            user2 = self.user_node(t2['user'])
            user2.tweeted.add(tweet)
            graph.push(user2)

    # Handle replied user here
    def replied_node(self, t):
        replied = Tweet()
        replied.id = t['in_reply_to_status_id']
        replied_user = self.user_node(None, t['in_reply_to_user_id'], t['in_reply_to_screen_name'])
        replied_user.tweeted.add(replied)
        graph.push(replied_user)
        return replied

    def tweet_node(self, t, id=None, text=None):
        tweet = Tweet()
        tweet.id = id or t['id']
        tweet.text = text or t['text']
        for m in t['entities']['user_mentions']:
            mentioned = self.user_node(m)
            tweet.mentioned.add(mentioned)
        return tweet

    def user_node(self, t, id=None, screen_name=None):
        user = User()
        user.id = id or t['id']
        user.username = screen_name or t['screen_name']
        return user


graph = Graph(password='1234')
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

listener = MyStreamListener()
listener.run_stream(auth=api.auth)
