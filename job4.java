import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class job4 {

  public static class Mapper1
       extends Mapper<LongWritable, Text, Text, NullWritable>{

     NullWritable one = NullWritable.get();
    private Text cited = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        /*String line = value.toString();
         String sv = line.substring(0,7);
          context.write(new Text(sv), one);*/

        String[] line = value.toString().split("\\s");
        if (line[0].charAt(0)!='#' && line[10].equals("404")){
            cited.set(line[4]);
         context.write(cited,NullWritable.get());
        }
    }
  }
/* public static class Reducer1
       extends Reducer<Text,NullWritable,Text,NullWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }*/

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");
    job.setJarByClass(job4.class);
    job.setMapperClass(Mapper1.class);
  //  job.setCombinerClass(Reducer1.class);
    //job.setReducerClass(Reducer1.class);
    job.setNumReduceTasks(0);
    /*[*/FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
    //job.setSortComparatorClass(SortKeyComparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}


