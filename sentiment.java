import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;

public class sentiment{
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private URI[] files;
		private HashMap<String,String> AFINN_map = new HashMap<String,String>();
		
		public void setup(Context context) throws IOException{
			files = DistributedCache.getCacheFiles(context.getConfiguration());
			System.out.println("files:"+ files);
			Path path = new Path(files[0]);
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
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		DistributedCache.addCacheFile(new URI("AFINN.txt"),conf);
		Job job = Job.getInstance(conf, "sentiment");
		job.setJarByClass(sentiment.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0:1);
	}
}
