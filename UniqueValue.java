package uniqueValue;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import java.util.*;
import java.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class uniqueValue {
	public static class uniqueValueMapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String v = value.toString();
			
			String customer = v.split(":")[0];
			String products = v.split(":")[1];
			
			int start = products.indexOf("[") + 1;
			int end = products.lastIndexOf("]");
			String product_all = products.substring(start, end);
			
			String[] products_list = product_all.split(",");
			
			for (String p: products_list) {
				String[] output = new String[2];
				output[0] = customer;
				output[1] = p;
				context.write(new Text(Arrays.toString(output)), new IntWritable(1));
			}
		}
	}
	
	public static class uniqueValueReducerOne extends Reducer<Text, IntWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	public static class uniqueValueMapperTwo extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String input = value.toString();
			int start = input.indexOf("[");
			int end = input.indexOf("]");

			String product = input.substring(start, end).split(",")[1];
			
			context.write(new Text(product), new IntWritable(1));
		}
	}
	
	public static class uniqueValueReducerTwo extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable v: value) {
				sum += v.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception{
				
		Configuration conf_1 = new Configuration();
		Job job_1 = Job.getInstance(conf_1, "unique value-stage1");
		job_1.setJarByClass(uniqueValue.class);
		
		job_1.setMapperClass(uniqueValueMapperOne.class);
		job_1.setReducerClass(uniqueValueReducerOne.class);
		job_1.setMapOutputKeyClass(Text.class);
		job_1.setMapOutputValueClass(IntWritable.class);
		job_1.setOutputKeyClass(Text.class);
		job_1.setOutputValueClass(NullWritable.class);
		
		job_1.setOutputFormatClass(TextOutputFormat.class);
		job_1.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job_1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job_1, new Path("first_output"));
		
		if (job_1.waitForCompletion(true)) {
			Configuration conf_2 = new Configuration();
			Job job_2 = Job.getInstance(conf_2, "unique value-stage2");
			job_2.setJarByClass(uniqueValue.class);
			
			job_2.setMapperClass(uniqueValueMapperTwo.class);
			job_2.setReducerClass(uniqueValueReducerTwo.class);
			job_2.setMapOutputKeyClass(Text.class);
			job_2.setMapOutputValueClass(IntWritable.class);
			job_2.setOutputKeyClass(Text.class);
			job_2.setOutputValueClass(IntWritable.class);
			
			job_2.setOutputFormatClass(TextOutputFormat.class);
			job_2.setInputFormatClass(TextInputFormat.class);
			
			TextInputFormat.addInputPath(job_2, new Path("first_output"));
			TextOutputFormat.setOutputPath(job_2, new Path(args[1]));
			
			System.exit(job_2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
