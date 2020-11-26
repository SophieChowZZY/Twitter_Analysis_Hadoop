# Twitter_Analysis_Hadoop
## For CS4480 Group Project
The project utilized Python, Hadoop and PySpark to do the analysis. This repo stores only the code for doing the Twitter Sentiment Analysis on Hadoop and Pig Latin.  Also, the fetch tweet using Twitter Developer Account (file:"fetch tweet") and snscrape on Python (file: "fetch_tweet_new"). These files are created and updated by me. <br><br>
The Data file contains raw data and some datafile pre-processing python codes *(remark: The trump data is too big, it cannot be uploaded to GitHub)*; Dict contains the three dictionaries for NLP: stopwords, WordNetLemma, and AFINN. <br><br>
The sentiment folder contains the MapReduce java file for doing all steps, named "tweet_analysis.java". Despite that, the old folder inside the sentiment folder contains separate MapReduce java code for each step as I first tried to separate the process into different files. <br>

### To run
<ol>
  <li>Connect the folder containing all the codes and files in this repo with the VirtualBox</li>
  <li>Create a new dir in HDFS named data and store the raw data and dictionaries into the dir</li>
  <li>Make tweet_analysis in Hadoop</li>
  <li>Run pig latin script for each candidate</li>
  <li>Extract result</li>
</ol>
