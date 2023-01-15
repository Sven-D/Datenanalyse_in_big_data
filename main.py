import pandas as pd
from pathlib import Path
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def getSentiment(text):
    sentiment = TextBlob(text.content).sentiment.polarity
    return float(sentiment)

tweet_path = Path('C:/Users/Sven/Desktop/replies_to_elon.json')

spark = SparkSession.builder.getOrCreate()
tweets = spark.read.json(str(tweet_path))

#removing unnecessary details like @ (maybe something else to remove?)
tweets = tweets.withColumn("content",F.regexp_replace("content", "(@[a-zA-Z0-9]*)", ""))
tweets = tweets.select(["content"])

rdd2=tweets.rdd.map(lambda x: getSentiment(x)) 

df2=rdd2.toDF(["content"])
df2.show()

