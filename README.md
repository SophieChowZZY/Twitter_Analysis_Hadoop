# Twitter_Analysis_Hadoop
## For CS4480 Group Project
This repo stores the code for doing the Twitter Sentiment Analysis on Hadoop and Pig Latin. Also, the fetch tweet using Twitter Developer Account and snscrape on Python. These files are created and updated by me. <br>
The Data file contains raw data and some datafile pre-processing python codes *(remark: The trump data is too big, it cannot be uploaded to GitHub)*; Dict contains the three dictionaries for NLP: stopwords, WordNetLemma, and AFINN. <br>
The sentiment folder contains the MapReduce java file for doing all steps, named "tweet_analysis.java". <br>
Despite that, the separate java files outside any folders are MapReduce for each particular job as I first tried to separate the process into different files. <br>

### To run
<ol>
  <li>Connect the folder containing all the codes and files in this repo with the VirtualBox</li>
  <li>Create a new dir in HDFS named data and store the raw data and dictionaries into the dir</li>
  <li>Make tweet_analysis in Hadoop</li>
  <li>Run pig latin script for each candidate</li>
  <li>Extract result</li>
</ol>

### Discussion on using Hadoop
Since our data was not very big, the MapReduce overheads could degrade the performance significantly. Also, since no one in the group knows Java, including me, it took me a lot of effort to understand and implement the MapReduce Java programming. Given the performance of Hadoop, it might not be worthwhile to do that. Nevertheless, it was an excellent chance for me to learn and practice using Java.<br> 

### End words
It is happy to learn a lot of things within one semester. Although I was surprised that in the end, all these parallel computing fancy jargons might not beat the very beginning C++ in performance, it was a pleasant journey to known the development of the technology. :)
