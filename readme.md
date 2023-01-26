# What is the Goal of this project? 

Performing a sentiment analysis that serves as an introduction to natural language processing. 

___

# What does the Script do? 

The script feeds a JSON (Path needs to be set) containing any amount of tweets into Spark a Spark Dataframe, does some cleaning then performs a lexicon based sentiment analysis. It does so, by calculating the polarity of every tweet, ranging between -1 (negative) to 1 (positive). The results containing the tweet contents, as well as the polarity, will then be written to a folder (Path needs to be set) multiple .csv files (1 per partition). 

The Tweets were scraped with snscrape (https://github.com/JustAnotherArchivist/snscrape). 

A data sample containing just a couple of tweets adressed to Elon Musk will be provided in this repository. 
