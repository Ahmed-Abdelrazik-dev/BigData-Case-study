import tweepy as tw
from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
from datetime import datetime

consumer_key = 'vKxYzdyW3jQtwRyfSC5NHItF5'
consumer_secret = '01W4YmQbmzscKNj7FNDNDxBSSuLx9rW6BWh7jyYcoc4PtYp9TP'
access_token = '1385704300891058187-4jStql55dprbl9tahjE4c0OeRWCj9V'
access_token_secret = 'qmGwuJV2OF2KfcS2HiZ4MUtPW1JfojsXgY9cYHrsEaHOw'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tw.API(auth , wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

def normalize_timestamp(time):
    mytime = datetime.strptime(time , "%Y-%m-%d %H:%M:%S")
    return(mytime.strftime("%Y-%m-%d %H:%M:%S"))
    
producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'])
topic_name = "Twitter_test35"

checklist = []

search_words = "#covid21"
date_since = "2019-10-01"

tweets = tw.Cursor(api.search,
                   q=search_words,
                   since=date_since,
                   lang="en").items()
while True:
    for r in tweets:
        if r not in checklist:
            print(type(tweets))
            for tweet in tweets:
                tweet_id = tweet.id_str
                text = tweet.text
                created_at = normalize_timestamp(str(tweet.created_at))
                followers_count = tweet.user.followers_count
                user_location = tweet.user.location
                favorite_count = tweet.favorite_count
                retweet_count = tweet.retweet_count

                record = ''
                record += str(tweet_id)
                record += ';'
                record += str(text)
                record += ';'
                record += str(created_at)
                record += ';'
                record += str(followers_count)
                record += ';'
                record += str(user_location)
                record += ';'
                record += str(favorite_count)
                record += ';'
                record += str(retweet_count)
                record += ';'
                print(type(record))
                print(record)
                checklist.append(r)
                producer.send(topic_name, str.encode(record))
                sleep(5)
                
        else:
            print("No more tweets")
            sleep(20)
