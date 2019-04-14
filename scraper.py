import os
import tweepy
import time
from neo4j import GraphDatabase
from dotenv import load_dotenv
from pprint import pprint
# from py2neo import Graph
# from objects import MyStreamListener
# from retrying import retry

load_dotenv()

consumer_token = os.getenv('CONSUMER_TOKEN')
consumer_secret = os.getenv('CONSUMER_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')

# this uses neo4j python driver
class AnotherStreamListener(tweepy.StreamListener):
    STATEMENT = ("UNWIND {tweets} AS t\n" +
            "WITH t,\n" +
            "     t.entities AS e,\n" +
            "     t.user AS u,\n" +
            "     t.retweeted_status AS retweet,\n" +
            "     t.quoted_status AS quoted\n" +
            "WHERE t.id is not null " +
            "MERGE (tweet:Tweet {id:t.id})\n" +
            "SET tweet.text = t.text,\n" +
            "    tweet.created = t.created_at,\n" +
            "    tweet.favorites = t.favorite_count\n" +
            "MERGE (user:User {id:u.id})\n" +
            "SET user.name = u.name,\n" +
            "    user.username = u.screen_name,\n" +
            "    user.location = u.location,\n" +
            "    user.followers = u.followers_count,\n" +
            "    user.following = u.friends_count,\n" +
            "    user.statuses = u.statuses_count,\n" +
            "    user.profile_image_url = u.profile_image_url\n" +
            "MERGE (user)-[:POSTED]->(tweet)\n" +
            "FOREACH (m IN e.user_mentions |\n" +
            "  MERGE (mentioned:User {id:m.id})\n" +
            "  ON CREATE SET mentioned.name = m.name,\n" +
            "                mentioned.username = m.screen_name\n" +
            "  MERGE (tweet)-[:MENTIONED]->(mentioned)\n" +
            ")\n" +
            "FOREACH (r IN [r IN [t.in_reply_to_status_id] WHERE r IS NOT NULL] |\n" +
            "  MERGE (reply_tweet:Tweet {id:r})\n" +
            "  MERGE (replier:User {id:t.in_reply_to_user_id})\n" +
            "  ON CREATE SET replier.username = t.in_reply_to_screen_name\n" +
            "  MERGE (tweet)-[:REPLIED_TO]->(reply_tweet)\n" +
            "  MERGE (replier)-[:POSTED]->(reply_tweet)\n" +
            ")\n" +
            "FOREACH (quoted_id IN [x IN [quoted.id] WHERE x IS NOT NULL] |\n" +
            "    MERGE (quoted_tweet:Tweet {id:quoted_id})\n" +
            "    ON CREATE SET quoted_tweet.text = quoted.text,\n" +
            "                  quoted_tweet.created = quoted.tweet.created_at\n" +
            "    MERGE (quoted_user:User {id:quoted.user.id})\n" +
            # We could set more props considering retweet_status and quoted_status are full tweet objects
            "    ON CREATE SET quoted_user.name = quoted.user.name,\n" +
            "                  quoted_user.username = quoted.user.screen_name\n" +
            "    MERGE (tweet)-[:RETWEETED]->(quoted_tweet)\n" +
            "    MERGE (quoted_user)-[:POSTED]->(quoted_tweet)\n" +
            ")\n" +
            "FOREACH (retweet_id IN [x IN [retweet.id] WHERE x IS NOT NULL] |\n" +
            "    MERGE (retweet_tweet:Tweet {id:retweet_id})\n" +
            "    ON CREATE SET retweet_tweet.text = retweet.text,\n" +
            "                  retweet_tweet.created = retweet.tweet.created_at\n" +
            "    MERGE (retweet_user:User {id:retweet.user.id})\n" +
            "    ON CREATE SET retweet_user.name = retweet.user.name,\n" +
            "                  retweet_user.username = retweet.user.screen_name\n" +
            "    MERGE (tweet)-[:RETWEETED]->(retweet_tweet)\n" +
            "    MERGE (retweet_user)-[:POSTED]->(retweet_tweet)\n" +
            ")")

    def __init__(self):
        super().__init__()
        self.count = 1
        self.db = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
        with self.db.session() as db:
            db.run("CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE")
            db.run("CREATE CONSTRAINT ON (u:User) ASSERT u.id IS UNIQUE")

    def on_status(self, tweet):
        if self.count == 10000:
            return False
        print("==================COUNT: {}==================".format(self.count))
        print(tweet.text)
        try:
            self.run_transaction(tweet)
            self.count += 1
        except Exception as e:
            print("ERROR: ",e)

    def on_error(self, status_code):
        print("Error code: ", status_code)

    def run_stream(self,auth):
        stream = tweepy.Stream(auth=auth, listener=self, timeout=5, retry_420_start=5)
        try:
            print("AnotherStream starting...")
            stream.filter(locations=[-130.56,23.59,-77.09,48.77])
        except Exception as e:
            print(e)
            print(e.__doc__)

    def run_transaction(self, t):
        t = t._json
        with self.db.session() as db:
            result = db.run(self.STATEMENT, {"tweets": t})
            print(result.summary().notifications)
            pprint(result.data())


# graph = Graph(password='1234')
auth = tweepy.OAuthHandler(consumer_token, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

listener = AnotherStreamListener()
listener.run_stream(auth=api.auth)
