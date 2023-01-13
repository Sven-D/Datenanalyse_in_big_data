import pandas as pd
from pathlib import Path
import textblob 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



tweet_path = Path('C:/Users/Sven/Desktop/replies_to_elon.json')

spark = SparkSession.builder.getOrCreate()
tweets = spark.read.json(str(tweet_path))

#removing unnecessary details like @ (maybe something else to remove?)
tweets = tweets.withColumn("content",F.regexp_replace("content", "(@[a-zA-Z0-9]*)", ""))


tweets.show()
tweets.select("content").show()




