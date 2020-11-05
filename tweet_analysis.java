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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/*

	Author: Sophie Zhiyu Zhou
	Procedures:
		1. Pre-processing: replace all url, #, @xxxx, and special characters 
			from the tweet text with empty string. Convert it to lower case.
		2. Lemmatization: Reduce the dimensionality of the tweets.
			(word pairs are in file "WordNetLemma.txt")
		3. Remove stopwords: remove english stopwrods from each tweet text. 
			(stopwords are in file "stopwords.txt")
		4. Sentiment: calculate sentiment for each tweet
			(sentiment words are in file "AFINN.txt")

*/

public class tweet_analysis{

	/*-----------------------------------Main Driver------------------------------------------*/
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		

		//Set Path
		Path inputPath = new Path(args[0]);
		///Pre-processing
		Path preInputPath = inputPath;
		Path preOutputPath = new Path("result/Preprocessing");
		///Lemmatization
		Path lemInputPath = preOutputPath;
		Path lemOutputPath = new Path("result/Lemmatize");
		///Stopwords
		Path stpInputPath = lemOutputPath;
		Path stpOutputPath = new Path("result/Stopwords");
		///Sentiment
		Path senInputPath = stpInputPath;
		Path senOutputPath = new Path("result/Sentiment");

		//Check output existence
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(preOutputPath))
			hdfs.delete(preOutputPath, true);
		if (hdfs.exists(stpOutputPath))
			hdfs.delete(stpOutputPath, true);
		if (hdfs.exists(lemOutputPath))
			hdfs.delete(lemOutputPath, true);
		if (hdfs.exists(senOutputPath))
			hdfs.delete(senOutputPath, true);

		//Pre-processing
		long pre_startTime = System.currentTimeMillis();
	
		Job jobPreprocess = Job.getInstance(conf, "preprocess");
		
		jobPreprocess.setNumReduceTasks(0);
		jobPreprocess.setJarByClass(tweet_analysis.class);
		jobPreprocess.setMapperClass(PMapper.class);
		jobPreprocess.setOutputKeyClass(Text.class);
		jobPreprocess.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobPreprocess, preInputPath);
		FileOutputFormat.setOutputPath(jobPreprocess, preOutputPath);
		jobPreprocess.waitForCompletion(true);

		long pre_endTime = System.currentTimeMillis();
		

		//Remove stopwords
		long stp_startTime = System.currentTimeMillis();

		DistributedCache.addCacheFile(new URI("stopwords.txt"),conf);

		Job jobStopwords = Job.getInstance(conf, "stopwords");
		
		jobStopwords.setNumReduceTasks(0);
		jobStopwords.setJarByClass(tweet_analysis.class);
		jobStopwords.setMapperClass(SMapper.class);
		jobStopwords.setOutputKeyClass(Text.class);
		jobStopwords.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobStopwords, stpInputPath);
		FileOutputFormat.setOutputPath(jobStopwords, stpOutputPath);
		jobStopwords.waitForCompletion(true);

		long stp_endTime = System.currentTimeMillis();
		

		//Lemmatizer
		long lem_startTime = System.currentTimeMillis();

		DistributedCache.addCacheFile(new URI("WordNetLemma.txt"),conf);

		Job jobLemmatize = Job.getInstance(conf, "lemmatize");

		jobLemmatize.setNumReduceTasks(0);
		jobLemmatize.setJarByClass(tweet_analysis.class);
		jobLemmatize.setMapperClass(LMapper.class);
		jobLemmatize.setOutputKeyClass(Text.class);
		jobLemmatize.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobLemmatize, lemInputPath);
		FileOutputFormat.setOutputPath(jobLemmatize, lemOutputPath);
		jobLemmatize.waitForCompletion(true);

		long lem_endTime = System.currentTimeMillis();
		

		//Sentiment
		long sen_startTime = System.currentTimeMillis();

		DistributedCache.addCacheFile(new URI("AFINN.txt"),conf);

		Job jobSentiment = Job.getInstance(conf, "sentiment");
		
		jobSentiment.setJarByClass(tweet_analysis.class);
		jobSentiment.setMapperClass(SMMapper.class);
		jobSentiment.setReducerClass(SMReducer.class);
		jobSentiment.setOutputKeyClass(Text.class);
		jobSentiment.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(jobSentiment, senInputPath);
		FileOutputFormat.setOutputPath(jobSentiment, senOutputPath);
		jobSentiment.waitForCompletion(true);

		long sen_endTime = System.currentTimeMillis();
		

		//Print execution time
		System.out.println("Pre-processing total execution time: " + (pre_endTime - pre_startTime));
		System.out.println("Stopwords total execution time: " + (stp_endTime - stp_startTime));
		System.out.println("Lemmatizer total execution time: " + (lem_endTime - lem_startTime));
		System.out.println("Sentiment total execution time: " + (sen_endTime - sen_startTime) );
		System.exit(0);
	}



	/*-----------------------------------Pre-procrssing------------------------------------------*/
	//Pre-processing Mapper
	public static class PMapper extends Mapper<Object, Text, Text, NullWritable>{
		private Text word = new Text();
		
		public void map(Object key, Text value, 
			Context context ) throws IOException,InterruptedException{
			String line = value.toString();
			String[] tuple = line.split("\\n");
			for(int i=0;i<tuple.length; i++){
				tuple[i] = tuple[i].replaceAll("^RT[\\s]+","");
				tuple[i] = tuple[i].replace("#","");
				tuple[i] = tuple[i].replaceAll("@[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("https?://[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("www\\.[^\\s]+", "");
		      	tuple[i] = tuple[i].replaceAll("[^a-zA-Z0-9-]+", " ");
		      	String[] splits = tuple[i].split(" ");
				if(splits.length > 1){
					tuple[i] = tuple[i].toLowerCase();
					word.set(tuple[i]);
					context.write(word, NullWritable.get());
				}
			}
		}
	}

	/*-----------------------------------Remove Stop Words------------------------------------------*/
	public static class SMapper extends Mapper<Object, Text, Text, NullWritable>{
        private URI[] files;
        private ArrayList<String> stword = new ArrayList<String>();
        
        public void setup(Context context) throws IOException{
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("files:"+ files);
            Path path = new Path(files[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line="";
            while((line = br.readLine())!=null){
                stword.add(line);
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
                for(int j=0; j<splits.length; j++){
                    if(stword.contains(splits[j])){
                        splits[j]=null;
                    }
                }
                List<String> list = new ArrayList<String>();

                for(String s : splits) {
                   if(s != null) {
                      list.add(s);
                   }
                }
                splits = list.toArray(new String[list.size()]);
                if(splits.length > 1){
					String stStr = String.join(" ", splits);
	                word.set(stStr);
	                context.write(word, NullWritable.get());  
				}
            }
        }
    }


    /*-----------------------------------Lemmatization------------------------------------------*/
    public static class LMapper extends Mapper<Object, Text, Text, NullWritable>{
        private URI[] files;
        private HashMap<String,String> WordNet_map = new HashMap<String,String>();
        
        public void setup(Context context) throws IOException{
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("files:"+ files);
            Path path = new Path(files[1]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line="";
            while((line = br.readLine())!=null){
                String splits[] = line.split(" ");
                for(int i=0; i<splits.length-1; i++){
                	WordNet_map.put(splits[i],splits[splits.length-1]);
                }
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
                for(int j=0; j<splits.length; j++){
                    if(WordNet_map.containsKey(splits[j])){
                        String x=WordNet_map.get(splits[j]);
                        splits[j]=x;
                    }
                }
                String lemmaStr = String.join(" ", splits);
                word.set(lemmaStr);
                context.write(word, NullWritable.get());  
            }
        }
    }


    /*-----------------------------------Sentiment------------------------------------------*/
    public static class SMMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private URI[] files;
		private HashMap<String,String> AFINN_map = new HashMap<String,String>();
		
		public void setup(Context context) throws IOException{
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

				if(sentiment_sum>0){
					word.set("Positive");
					context.write(word, one);
				}else if(sentiment_sum<0){
					word.set("Negative");
					context.write(word, one);
				}else{
					word.set("Neutral");
					context.write(word, one);
				}
			}
		}
	}

	public static class SMReducer
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
}
