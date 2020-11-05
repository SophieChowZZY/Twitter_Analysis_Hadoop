TEMP=wc_tmp
INPUTPATH=data/trump.txt
KVAL=10

t_analysis_1: t_analysis_1.jar
	hadoop jar $< $(basename $<) $(INPUTPATH) 

tweet_analysis: tweet_analysis.jar
	hadoop jar $< $(basename $<) $(INPUTPATH) 

preprocess: preprocess.jar 
	hadoop jar $< $(basename $<) $(INPUTPATH) 

stopword: stopword.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) 

lemma: lemma.jar
	hadoop jar $< $(basename $<) $(INPUTPATH) 

TFIDF: TFIDF.jar
	hdfs dfs -rm -r -f $(result/WordCount)
	hdfs dfs -rm -r -f $(result/DocSize)
	hdfs dfs -rm -r -f $(result/TFIDF)
	hadoop jar $< $(basename $<) $(INPUTPATH)

sentiment: sentiment.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) 

WordCount: WordCount.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(OUTPUT)

MaxWordCount: MaxWordCount.jar
	hdfs dfs -rm -r -f $(OUTPUT) $(WCTMP)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(WCTMP) $(OUTPUT)

TopKWordCount: TopKWordCount.jar
	hdfs dfs -rm -r -f $(OUTPUT) $(WCTMP)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(WCTMP) $(OUTPUT) $(KVAL)





%.jar: %.java
	hadoop com.sun.tools.javac.Main $<
	jar cf $@ $(basename $<)*.class

clean:
	rm -f *.class *.jar
	hdfs dfs -rm -r -f $(OUTPUT) $(WCTMP)

	
