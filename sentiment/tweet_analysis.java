import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*

	

*/

public class tweet_analysis{

	/*-----------------------------------Main Driver------------------------------------------*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		

		//Set Path
		Path inputPath = new Path(args[0]);

		///Lexicon-based process
		Path preInputPath = inputPath;
		Path preOutputPath = new Path("result/Preprocess");
		
		///Sentiment
		Path senInputPath = preOutputPath;
		Path senOutputPath = new Path("result/Sentiment");

		///WC
		Path wcInputPath = preOutputPath;
		Path wcOutputPath = new Path("result/WordCount");

		Path tfidfOutputPath = new Path("result/TFIDF");


		//Check output existence
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(preOutputPath))
			hdfs.delete(preOutputPath, true);
		if (hdfs.exists(senOutputPath))
			hdfs.delete(senOutputPath, true);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(tfidfOutputPath))
			hdfs.delete(tfidfOutputPath, true);

		//Lexicon-based process
		long pre_startTime = System.currentTimeMillis();

		DistributedCache.addCacheFile(new URI("data/WordNetLemma.txt"),conf);
		DistributedCache.addCacheFile(new URI("data/stopwords.txt"),conf);
	
		Job jobPreprocess = Job.getInstance(conf, "preprocess");

		jobPreprocess.setJarByClass(tweet_analysis.class);
		jobPreprocess.setMapperClass(PMapper.class);
		jobPreprocess.setReducerClass(PReducer.class);
		jobPreprocess.setOutputKeyClass(Text.class);
		jobPreprocess.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobPreprocess, preInputPath);
		FileOutputFormat.setOutputPath(jobPreprocess, preOutputPath);

		jobPreprocess.waitForCompletion(true);

		long pre_endTime = System.currentTimeMillis();


		//Sentiment
		long sen_startTime = System.currentTimeMillis();

		DistributedCache.addCacheFile(new URI("data/AFINN.txt"),conf);

		Job jobSentiment = Job.getInstance(conf, "sentiment");
		
		jobSentiment.setJarByClass(tweet_analysis.class);
		jobSentiment.setMapperClass(SMMapper.class);
		jobSentiment.setReducerClass(IntSumReducer.class);
		jobSentiment.setOutputKeyClass(Text.class);
		jobSentiment.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobSentiment, senInputPath);
		FileOutputFormat.setOutputPath(jobSentiment, senOutputPath);
		jobSentiment.waitForCompletion(true);

		long sen_endTime = System.currentTimeMillis();


		//TFIDF
		long wc_startTime = System.currentTimeMillis();

		////Step 1: Count the word occurence in a document
		Job jobWordcount = Job.getInstance(conf, "wordcount");
		jobWordcount.setJarByClass(tweet_analysis.class);
		jobWordcount.setMapperClass(WCMapper.class);
		jobWordcount.setReducerClass(IntSumReducer.class);
		jobWordcount.setOutputKeyClass(Text.class);
		jobWordcount.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobWordcount, wcInputPath);
		FileOutputFormat.setOutputPath(jobWordcount, wcOutputPath);
		jobWordcount.waitForCompletion(true);

		long wc_endTime = System.currentTimeMillis();
		
		//Print execution time
		System.out.println("Pre-processing total execution time: " + (pre_endTime - pre_startTime));
		System.out.println("Sentiment total execution time: " + (sen_endTime - sen_startTime) );
		System.out.println("Word Count total execution time: " + (wc_endTime - wc_startTime) );

		System.exit(0);
	}



	/*-----------------------------------Pre-procrssing------------------------------------------*/
	//Pre-processing Mapper
	public static class PMapper extends Mapper<Object, Text, Text, NullWritable>{
		private Text word = new Text();
		
		//Set up 
		private URI[] files;
		private HashMap<String,String> WordNet_map = new HashMap<String,String>();
		private ArrayList<String> stword = new ArrayList<String>();
		
        
        public void setup(Context context) throws IOException, InterruptedException{
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("files:"+ files);
            Path path_lemma = new Path(files[0]);
            Path path_stpword = new Path(files[1]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in_lemma = fs.open(path_lemma);
            FSDataInputStream in_stpword = fs.open(path_stpword);
            BufferedReader br_lemma = new BufferedReader(new InputStreamReader(in_lemma));
            BufferedReader br_stpword = new BufferedReader(new InputStreamReader(in_stpword));
            
            //read WordNetLemma
            String line="";
            while((line = br_lemma.readLine())!=null){
                String splits[] = line.split(" ");
                for(int i=0; i<splits.length-1; i++){
                	WordNet_map.put(splits[i],splits[splits.length-1]);
                }
            }
            br_lemma.close();
            in_lemma.close();

            //read stopwords
            line="";
            while((line = br_stpword.readLine())!=null){
                stword.add(line);
            }
            br_stpword.close();
            in_stpword.close();
        }

		
		
		public void map(Object key, Text value, 
			Context context ) throws IOException,InterruptedException{
			String line = value.toString();
			String[] tuple = line.split("\\n");
			for(int i=0;i<tuple.length; i++){
				String[] splits = tuple[i].split(" ");
				String date = splits[0];
				tuple[i] = tuple[i].replaceAll("^RT[\\s]+","");
				tuple[i] = tuple[i].replace("#","");
				tuple[i] = tuple[i].replaceAll("@[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("https?://[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("www\\.[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("[^a-zA-Z]+", " ");
		      	tuple[i] = tuple[i].toLowerCase();
				splits = tuple[i].split(" ");
				if(splits.length > 1){
					//check base form and stop words
					List<String> list = new ArrayList<String>();
	                for(int j=0; j<splits.length; j++){
	                	if(WordNet_map.containsKey(splits[j])){
                        	String x=WordNet_map.get(splits[j]);
                        	splits[j]=x;
                    	}
	                    if(stword.contains(splits[j])){
	                        splits[j]=" ";
	                    }
	                    if(splits[j].length()>2) {
	                    	list.add(splits[j]);
	                   }
	                }

	                //Convert splits back to string and write result
	                splits = list.toArray(new String[list.size()]);
	                
	                if(splits.length > 0){
	                	String stStr = String.join(" ", splits);
	                	stStr = stStr.trim();
	                	word.set(date + " " + stStr);
	                	context.write(word, NullWritable.get());
					}
				}
			}
		}
	}

	public static class PReducer
       extends Reducer<Text,Text,Text,NullWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values,Context context) 
	    	throws IOException, InterruptedException {
	    	
	      	context.write(key, NullWritable.get());
	    }
  	}

    /*-----------------------------------Sentiment------------------------------------------*/
    public static class SMMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private URI[] files;
		private HashMap<String,String> AFINN_map = new HashMap<String,String>();
		
		public void setup(Context context) throws IOException, InterruptedException{
			files = DistributedCache.getCacheFiles(context.getConfiguration());
			System.out.println("files:"+ files);
			Path path = new Path(files[2]);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line="";
			while((line = br.readLine())!=null){
				String splits[] = line.split("\t");
				AFINN_map.put(splits[0],splits[1]);
			}
			br.close();
			in.close();
		}
		
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context ) throws IOException,InterruptedException{
			String line = value.toString();
			String[] tuple = line.split("\\n");
			for(int i=0;i<tuple.length; i++){
				String[] splits = tuple[i].split(" ");
				int sentiment_sum=0;
				for(String word:splits){
					if(AFINN_map.containsKey(word)){
						Integer x=new Integer(AFINN_map.get(word));
						sentiment_sum+=x;
					}
				}
				float avg_sentiment = sentiment_sum/splits.length;
				if(avg_sentiment>0){
					word.set(splits[0] + " Positive");
					context.write(word, one);
				}else if(avg_sentiment<0){
					word.set(splits[0] + " Negative");
					context.write(word, one);
				}else{
					word.set(splits[0] + " Neutral");
					context.write(word, one);
				}
				
			}
		}
	}

	public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable result = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      result.set(sum);
	      context.write(key, result);
	    }
  	}


  	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable>{
  		private final static IntWritable one = new IntWritable(1);

  		private Text word = new Text();
		
		public void map(Object key, Text value, Context context ) throws IOException,InterruptedException{
			String line = value.toString();
			String[] tuple = line.split("\\n");
			int[] docs = {15, 20, 25, 30};
			for(int i=0;i<tuple.length; i++){
				int doc = 15;
				String[] splits = tuple[i].split(" ");
				String[] date = splits[0].split("/");
				int present = Integer.valueOf(date[1]);
				for (int d:docs){
					if(Math.abs(d-present)<5){
						doc = d;
						break;
					}
					else{
						doc = 30;
					}
				}
				for(int j=1; j<splits.length; j++){
					word.set(doc + "\t" + splits[j]);
					context.write(word, one);
				}
			}
		}
	}

}
