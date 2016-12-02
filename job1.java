import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.util.GenericOptionsParser;    
    public class job1 {

  public static class Mapper1
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text cited = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String [] line = value.toString().split("\\s",14);
           if (line.length>10 &&line[8]!= "64.131.111.16"){
                cited= new Text(line[8]);
            context.write(cited, one);
    }
    }
  }

  public static class Mapper2 extends Mapper<Text,IntWritable,Text,IntWritable> {
   // @Override
        private IntWritable result = new IntWritable();
    public void map(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static class Reducer1
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
     int maxValue = Integer.MIN_VALUE;
    for (IntWritable value : values) {
      maxValue = Math.max(maxValue, value.get());
    }
    context.write(key, new IntWritable(maxValue));
    }
  }
public static void main(String[] args) throws Exception {
    Configuration config = new Configuration();
    String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
    if (otherArgs.length != 2) {
    System.err.print("Useage: wordcount <in> <out>");
    System.exit(2);
      }
    Job job = Job.getInstance();
    ChainMapper chainMapper = new ChainMapper();
    Configuration mapper1Config=new Configuration(false);
    chainMapper.addMapper(
        job,
        Mapper1.class,
        LongWritable.class,
        Text.class,
        Text.class,
        IntWritable.class,
        mapper1Config
        );
    Configuration mapper2Config=new Configuration(false);
    chainMapper.addMapper(
        job,
        Mapper2.class,
        Text.class,
        IntWritable.class,
        Text.class,
        IntWritable.class,
        mapper2Config
        );
    job.setJarByClass(job1.class);
   // job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    job.setOutputKeyClass(Text.class);
   // job.setNumReduceTasks(0);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//output:104.197.195.206 205464

