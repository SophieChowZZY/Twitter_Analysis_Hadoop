# Twitter_Analysis_Hadoop
## For CS4480 Group Project
The project utilized Python, Hadoop and PySpark to do the analysis. This repo stores only the code for doing the Twitter Sentiment Analysis on Hadoop and Pig Latin.  Also, the fetch tweet using Twitter Developer Account (file:"fetch tweet") and snscrape on Python (file: "fetch_tweet_new"). <br><br>
The Data file contains some datafile pre-processing python codes; Dict contains the three dictionaries for NLP: stopwords, WordNetLemma, and AFINN. <br><br>
The sentiment folder contains the MapReduce java file for doing all steps, named "tweet_analysis.java". Despite that, the old folder inside the sentiment folder contains separate MapReduce java code for each step as I first tried to separate the process into different files. <br>
