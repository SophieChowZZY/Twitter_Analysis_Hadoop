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
import org.apache.hadoop.io.IntWritable;

public class preprocess{
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
				if(splits.length != 1){
					tuple[i] = tuple[i].toLowerCase();
					word.set(tuple[i]);
					context.write(word, NullWritable.get());
				}
			}
		}
	}

  	public static void main(String[] args) throws Exception {
		long pre_startTime = System.currentTimeMillis();
		
		Configuration conf = new Configuration();
		
		Path inputPath = new Path(args[0]);
		Path preInputPath = inputPath;
		Path preOutputPath = new Path("result/Preprocessing");

		Job jobPreprocess = Job.getInstance(conf, "preprocess");
		
		jobPreprocess.setNumReduceTasks(0);
		jobPreprocess.setJarByClass(preprocess.class);
		jobPreprocess.setMapperClass(PMapper.class);
		jobPreprocess.setOutputKeyClass(Text.class);
		jobPreprocess.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(jobPreprocess, preInputPath);
		FileOutputFormat.setOutputPath(jobPreprocess, preOutputPath);
		jobPreprocess.waitForCompletion(true);

		long pre_endTime = System.currentTimeMillis();
		System.out.println("Pre-processing total execution time: " + (pre_endTime - pre_startTime) );
	}
 }