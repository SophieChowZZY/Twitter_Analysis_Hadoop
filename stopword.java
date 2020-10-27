import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
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
import java.util.*;

public class stopword{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable>{
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
                   if(s != null && s.length() > 1) {
                      list.add(s);
                   }
                }
                splits = list.toArray(new String[list.size()]);
                String stStr = String.join(" ", splits);
                word.set(stStr);
                context.write(word, NullWritable.get());  
            }
        }
    }

    public static class StReducer
       extends Reducer<Text,IntWritable,Text,NullWritable> {

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI("stopwords.txt"),conf);
        Job job = Job.getInstance(conf, "stopword");
        job.setJarByClass(stopword.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(StReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}