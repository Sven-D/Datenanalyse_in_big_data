# What is the Goal of this project? 

Performing a sentiment analysis that serves as an introduction to natural language processing. The idea behind the project is to investigate wheter or not Elon Musk's popularity has been on the decline. Sadly Twitter doesnt provide an API offering large amounts of historical data, That is why I've used just a couple of days worth of tweets as a sample.  

___

# What does the Script do? 

The script feeds a JSON "sample_tweets" placed in the project folder containing any amount of tweets into Spark a Spark Dataframe, does some cleaning then performs a lexicon based sentiment analysis. It does so, by calculating the polarity of every tweet, ranging between -1 (negative) to 1 (positive). additionally all tweets are filtered, so only english tweets are considered, since Textblob's multilanguage support is limited.  

The results containing the tweet contents, as well as the polarity, will then be written to a folder "result" containing multiple .csv files (1 per partition). 

The Tweets were scraped with snscrape (https://github.com/JustAnotherArchivist/snscrape). 

A data sample containing just a couple of tweets adressed to Elon Musk will be provided in this repository. 
