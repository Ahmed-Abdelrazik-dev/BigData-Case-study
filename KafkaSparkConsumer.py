import findspark
findspark.init()
import pyspark.sql.functions
from pyspark.sql.functions import udf
import tweepy as tw

from pyspark.sql.types import StringType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from textblob import TextBlob
import pandas as pd


if __name__=="__main__":

    consumer_key = 'vKxYzdyW3jQtwRyfSC5NHItF5'
    consumer_secret = '01W4YmQbmzscKNj7FNDNDxBSSuLx9rW6BWh7jyYcoc4PtYp9TP'
    access_token = '1385704300891058187-4jStql55dprbl9tahjE4c0OeRWCj9V'
    access_token_secret = 'qmGwuJV2OF2KfcS2HiZ4MUtPW1JfojsXgY9cYHrsEaHOw'

    auth = tw.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tw.API(auth , wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    spark = SparkSession\
        .builder\
        .appName("Big Data Case Study")\
        .master("local")\
        .config("spark.driver.host","localhost")\
        .getOrCreate()

    topicStream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers","sandbox-hdp.hortonworks.com:6667") \
        .option("subscribe", "Twitter_test35")\
        .load()

    df1 = (topicStream
           .withColumn("key", topicStream["key"].cast(StringType()))
           .withColumn("value", topicStream["value"].cast(StringType())))

    df1 = df1.select("value")

    split_col = pyspark.sql.functions.split(df1['value'], ';')
    df1 = df1.withColumn("Tweet_ID", split_col.getItem(0))
    df1 = df1.withColumn("Tweet", split_col.getItem(1))
    df1 = df1.withColumn("Created_time", split_col.getItem(2))
    df1 = df1.withColumn("Followers_count", split_col.getItem(3))
    df1 = df1.withColumn("Location", split_col.getItem(4))
    df1 = df1.withColumn("Favorite_count", split_col.getItem(5))
    df1 = df1.withColumn("Retweet_count", split_col.getItem(6))
    df1 = df1.drop("value")

    # df1['sentiment'] = df1.select("Tweet").apply(lambda x: TextBlob(x).sentiment)
    print(type(df1))


    def get_tweet_sentiment(tweetText, tweetId):

        reply = ""
        message_analysis = ""
        # tweetID = df1['Tweet_ID']
        #print(reply, tweetId, "*****************************************************************")

        analysis = TextBlob(tweetText)
        if analysis.sentiment.polarity > 0:
            message_analysis = 'positive'
            reply = "Thank you for ur feedback"
            # api.update_status(status=reply, in_reply_to_status_id=tweetID, auto_populate_reply_metadata=True)

        elif analysis.sentiment.polarity == 0:
            message_analysis = 'neutral'
            reply = "It's okay"
            # api.update_status(status=reply, in_reply_to_status_id=tweetID, auto_populate_reply_metadata=True)
        else:
            message_analysis = 'negative'
            reply = "We will Contact you ASAP"
            # api.update_status(status=reply, in_reply_to_status_id=tweetID, auto_populate_reply_metadata=True)
        #print(reply, tweetId, "*****************************************************************")
        # try:
        #     api.update_status(status=reply, in_reply_to_status_id=tweetId, auto_populate_reply_metadata=True)
        # except tw.TweepError as error:
        #     if error.api_code == 187:
        #         # Do something special
        #         print('duplicate message')
        # else:
        #     print("**********************************ERROR****************************")

        return message_analysis


    udf_sentiment = udf(get_tweet_sentiment, StringType())
    df1 = df1.withColumn("Message", udf_sentiment("Tweet", "Tweet_ID"))




    query = df1 \
        .writeStream \
        .format("parquet")\
        .option("format", "append")\
        .outputMode("append")\
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/CaseStudy/OutputStream")\
        .option("checkpointLocation", "hdfs://sandbox-hdp.hortonworks.com:8020/CaseStudy/checkpoint")\
        .start()
    
    

    query.awaitTermination()
    spark.stop()