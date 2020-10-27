OUTPUT=result/J1
TEMP=wc_tmp
INPUTPATH=data/T.txt
KVAL=10

tfidf: tfidf.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(OUTPUT)

stopword: stopword.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(OUTPUT)

lemma: lemma.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(OUTPUT)

sentiment: sentiment.jar
	hdfs dfs -rm -r -f $(OUTPUT)
	hadoop jar $< $(basename $<) $(INPUTPATH) $(OUTPUT)

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

	
