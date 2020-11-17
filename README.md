# Twitter_Analysis_Hadoop
## For CS4480 Group Project
This repo stores the code for doing the Twitter Sentiment Analysis on Hadoop and Pig Latin. Also the fetch tweet using Twitter Developer Account and snscrape on Python. These files are created and updated by me. <br>
The Data file contains raw data and some datafile pre-processing python codes *(remark: The trump data is too big, it cannot be uploaded to GitHub)*; Dict contains the three dictionaries for nlp: stopwords, WordNetLemma, and AFINN. <br>
The sentiment folder contains MapReduce java file for doing all steps, which named "tweet_analysis.java". <br>
Despite that, the seperate java files outside any folders are MapReduce for each seperate job as I first tried to seperate the process into different files. <br>

### To run
<ol>
  <li>Connect the folder containing all the codes and files in this repo with the VirtualBox</li>
  <li>Create a new dir in HDFS named data and store the raw data and dictionaries into the dir</li>
  <li>Make tweet_analysis in Hadoop</li>
  <li>Run pig latin script for each candidate</li>
  <li>Extract result</li>
</ol>

### Discussion on using Hadoop
Since our data is not very big in size, the MapReduce overheads can significantly latent the performance. Also since no one in the group knows java including me, it takes me a lot of effort on understanding and implementing the MapReduce Java programming. Given the performance of Hadoop, it may not be worthwhile on doing that. Nevertheless, it is a great chance for me to learn and practice on using Java.<br> 

### End word for the course
It is happy to learn a lot of things within one semester of time. Although I was surprised that in the end all these parallel computing fancy jargons may not beat the very beginning C++ in performance, it was a pleasant journey to known the development of the technology. :)
