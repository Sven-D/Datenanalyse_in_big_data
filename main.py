import pandas as pd
from pathlib import Path
from textblob import TextBlob
from pyspark.sql.types import StructField, StructType, DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Defining a Method for the mapped rdd 

def getSentiment(text):
    sentiment = TextBlob(text.content).sentiment.polarity
    return float(sentiment)


tweet_path = Path('/home/oreganoantonio/Desktop/replies_to_elon.json')
result_path = '/home/oreganoantonio/Desktop/result'

spark = SparkSession.builder.getOrCreate()
tweets = spark.read.json(str(tweet_path))

#removing unnecessary user handles from messages and removing None-Types
tweets = tweets.withColumn("content",F.regexp_replace("content", "(@[a-zA-Z0-9]*)", ""))
tweets = tweets.filter("content is not NULL")


#Creating a resilient distributed dataset to parallelize polarity calculation, then transform rdd to dataframe and merge
rdd = tweets.select("content").rdd.map(lambda x: (x.content, getSentiment(x)))

#defining a schema for the resulting dataframe
schema = StructType([
    StructField("content",StringType(),True),
    StructField("content polarity",DoubleType(),True)
])

#converting the rdd to a dataframe and saving the result
polarity = rdd.toDF(schema)
polarity.write.csv(result_path)





