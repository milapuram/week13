import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class job3 {

  public static class Mapper1
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    //private Text cited = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        /*String line = value.toString();
         String sv = line.substring(0,7);
          context.write(new Text(sv), one);*/

        String[] line = value.toString().split("\\s");
        if (line[0].charAt(0) != '#' && !line[4].equals("/")){

          String sv = line[0];
          String sv1= sv.substring(0,7);
          String cited=line[4];
          context.write(new Text(sv + ":" + cited), one);
        }
    }
  }

  public static class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static int fvalue = 0;
        private static Text fkey = new Text();
        private static IntWritable max_value = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
          if(sum > fvalue)
          {
                  fvalue = sum;
                  fkey.set(key);
                  max_value.set(sum);
          }
    }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
                context.write(fkey, max_value);
        }
  }
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "job1");
    job.setJarByClass(job3.class);
    job.setMapperClass(Mapper1.class);
    job.setCombinerClass(Reducer1.class);
    job.setReducerClass(Reducer1.class);
    //job.setSortComparatorClass(SortKeyComparator.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
                         
