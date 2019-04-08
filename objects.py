from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom
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
