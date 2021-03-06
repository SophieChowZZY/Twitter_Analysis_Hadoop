trump 		= LOAD 'result/WordCount' AS (Doc:chararray, word:chararray, count:double);
tweetByDoc 	= GROUP trump BY Doc;
ttlByDoc 	= FOREACH tweetByDoc GENERATE group AS Doc, SUM(trump.count) AS DocSize;
words 		= GROUP trump BY word;
wordByDoc 	= FOREACH words GENERATE group AS word, (DOUBLE)COUNT(trump.Doc) AS numDoc;
temp 		= JOIN trump BY Doc, ttlByDoc BY Doc;
tfTable 	= FOREACH temp GENERATE $0 AS Doc,$1 as Word,$2/$4 AS TF;
temp 		= JOIN tfTable BY Word, wordByDoc BY word;
tfidfTable 	= FOREACH temp GENERATE $0 AS Doc, $1 AS Word, $2 AS TF, LOG((DOUBLE)15/$4) AS IDF;
resultTable = FOREACH tfidfTable GENERATE Doc, Word, TF*IDF AS TFIDF;
STORE resultTable INTO 'result/TFIDF';

