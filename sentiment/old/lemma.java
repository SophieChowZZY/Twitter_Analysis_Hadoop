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

public class lemma{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable>{
        private URI[] files;
        private HashMap<String,String> WordNet_map = new HashMap<String,String>();
        
        public void setup(Context context) throws IOException{
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            System.out.println("files:"+ files);
            Path path = new Path(files[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream in = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line="";
            while((line = br.readLine())!=null){
                String splits[] = line.split(" ");
                WordNet_map.put(splits[0],splits[1]);
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

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        DistributedCache.addCacheFile(new URI("WordNetLemma.txt"),conf);
        Job job = Job.getInstance(conf, "lemma");
        jobPreprocess.setNumReduceTasks(0);
        job.setJarByClass(lemma.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("result/lemma"));
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}