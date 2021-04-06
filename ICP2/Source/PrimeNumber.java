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

public class PrimeNumber {
	
	   public static class TokenizerMapper
       			extends Mapper<Object, Text, Text, IntWritable> {
		   private final static IntWritable one = new IntWritable(1);
		   private Text word = new Text();

		   public void map(Object key, Text value, Context context
		   ) throws IOException, InterruptedException {
		       StringTokenizer itr = new StringTokenizer(value.toString());
		       while (itr.hasMoreTokens()) {
		           word.set(itr.nextToken());
		           context.write(word, one);
		       }
		   }
		}
	   
	   
	   public static class IntSumReducer //Modified Reducer Class for prime number
       			extends Reducer<Text, IntWritable, Text, IntWritable> {
		   private IntWritable result = new IntWritable();
		   			
		   public void reduce(Text key, Iterable<IntWritable> values,
				 //Value with which the same key value can exist is passed to collection
		                      Context context
		   ) throws IOException, InterruptedException {
		       int sum = 0;
		       int i_key=Integer.parseInt(key.toString());
		       for (int i=2; i<i_key/2; i++) {
		           if(i_key%i==0){
		        //If certain number is divided by number except 1 and itself then it is not prime number		        	   
			           	sum=1;
			           	break;
		           }
		       }
		       //If sum is 0, then prime number and If 1, then not prime number		       
		       result.set(sum);
		       context.write(key, result);
		       }
		}
	   
	   public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "word count");
	        job.setJarByClass(PrimeNumber.class);
	        job.setMapperClass(TokenizerMapper.class);
	        job.setCombinerClass(IntSumReducer.class);
	        job.setReducerClass(IntSumReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	   

}
