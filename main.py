import pandas as pd
from pathlib import Path
from textblob import TextBlob
from langdetect import detect
from pyspark.sql.types import StructField, StructType, DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Defining a Method for the mapped rdd 
#Getting the polarity score for each row
def getSentiment(text):
    sentiment = TextBlob(text.content).sentiment.polarity
    return float(sentiment)

#Getting the language for each tweet, Exception is thrown when the language can't be estimated (Tweet only containing smileys for example) 
def getLang(text):
    try:
        lang = detect(text.content) 
    except Exception:
        lang = "error"
    return lang


tweet_Path = Path('Insert Path')
result_Path = ('Insert Path')

spark = SparkSession.builder.getOrCreate()
tweets = spark.read.json(str(tweet_Path))

#removing unnecessary user handles from messages, None-Types, and filtering out none-english tweets 
tweets = tweets.withColumn("content",F.regexp_replace("content", "(@[a-zA-Z0-9]*)", ""))
tweets = tweets.filter("content is not NULL")

#Creating a resilient distributed dataset to parallelize language detection/filtering and polarity calculation.
rdd = tweets.select("content").rdd
rdd = rdd.filter(lambda x: getLang(x) == "en")
rdd = rdd.map(lambda x: (x.content, getSentiment(x)))

#defining a schema for the resulting dataframe
schema = StructType([
    StructField("content",StringType(),True),
    StructField("content polarity",DoubleType(),True)
])

#converting the rdd to a dataframe and saving the result
polarity = rdd.toDF(schema)
polarity.write.csv(result_Path)






