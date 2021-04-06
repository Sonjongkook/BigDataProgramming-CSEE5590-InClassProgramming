import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

  public static class TokenizerMapper//Mapper Class
       extends Mapper<Object, Text, Text, IntWritable>{
	  //Input key, Input value, output key, output value	  

    private final static IntWritable one = new IntWritable(1);
    // Hadoop-only data type just like int   
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      //Seperate String with Token using tokenizer(default seperator is space)      
      while (itr.hasMoreTokens()) { //If there is more than one token implement function
        word.set(itr.nextToken()); // Send a parameter of key-value Reducer
        context.write(word, one); //Save as write file
      }
    }
  }

  public static class IntSumReducer //Reducer Class
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
    						    //Value with which the same key value can exist is passed to collection
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) { //Each object's value is fixed to 1
        sum += val.get(); // if it is same key then increase the sum 
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count"); // make instance of Job
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class); //Reduce: finish
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0])); //input dir
    FileOutputFormat.setOutputPath(job, new Path(args[1]));//output dir
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
