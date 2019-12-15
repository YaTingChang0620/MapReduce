package RelationalOperation;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.google.common.collect.Iterables;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import java.io.*;

public class Intersection {
	
	public static class IntersectionMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			IntWritable one = new IntWritable(1);
			context.write(value, one);
		}
	}
	
	public static class IntersectionReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException{
			int size = Iterables.size(value);
			if (size == 2) {
				context.write(key, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "intersection");
		
		
		job.setJarByClass(Intersection.class);
		
		job.setMapperClass(IntersectionMapper.class);
		job.setReducerClass(IntersectionReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
