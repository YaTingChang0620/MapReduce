package RelationalOperation;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

public class UnionAll {
	public static class UnionAllMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			IntWritable one = new IntWritable(1);
			context.write(one, value);
		}
	}
	
	public static class UnionAllReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
		public void reduce(IntWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			for (Text t: value) {
				context.write(t, NullWritable.get());
			}
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "union_all");

		job.setJarByClass(UnionAll.class);
		
		job.setMapperClass(UnionAllMapper.class);
		job.setReducerClass(UnionAllReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
