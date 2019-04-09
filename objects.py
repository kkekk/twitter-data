from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom
import tweepy
# NOT USED
class Tweet(GraphObject):
    id = Property()
    text = Property()

    retweeted = RelatedTo('Tweet')
    replied_to = RelatedTo('Tweet')
    mentioned = RelatedTo('User')

class User(GraphObject):
    id = Property()
    username = Property()

    tweeted = RelatedTo(Tweet)

# To be removed. This streamer uses py2neo with alot of issues with merging
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
            time.sleep(5)


    def on_error(self, status_code):
        print("Error code: ", status_code)

    # @retry(wait_fixed=5000)
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

        # include mentions here
        user.tweeted.add(tweet)
        print('here1')
        graph.push(user)

        if t.get('retweeted_status') or t.get('quoted_status'):
            # include mentions here
            t2 = t.get('retweeted_status') or t.get('quoted_status')
            retweet = self.tweet_node(t2)
            if t2.get('in_reply_to_status_id'):
                replied2 = self.replied_node(t2)
                tweet.replied_to.add(replied2)


            tweet.retweeted.add(retweet)
            user2 = self.user_node(t2['user'])
            user2.tweeted.add(tweet)
            print('here2')
            graph.push(user2)

    # Handle replied user here
    def replied_node(self, t):
        replied = Tweet()
        replied.id = t['in_reply_to_status_id']
        print('replied1')
        graph.merge(replied)
        replied_user = self.user_node(None, t['in_reply_to_user_id'], t['in_reply_to_screen_name'])
        print('replied2')
        graph.merge(replied_user)
        replied_user.tweeted.add(replied)
        return replied

    def tweet_node(self, t, id=None, text=None):
        tweet = Tweet()
        tweet.id = id or t['id']
        graph.merge(tweet)
        tweet.text = text or t['text']
        for m in t['entities']['user_mentions']:
            mentioned = self.user_node(m)
            tweet.mentioned.add(mentioned)
        return tweet

    def user_node(self, t, id=None, screen_name=None):
        user = User()
        user.id = id or t['id']
        print('user_node here')
        graph.merge(user)
        user.username = screen_name or t['screen_name']
        return user
