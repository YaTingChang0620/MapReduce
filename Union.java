package RelationalOperation;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.Path;

public class Union {
	
	public static class UnionMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
		public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			context.write(value, NullWritable.get());
		}
	}
	
	public static class UnionReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException{
			context.write(key, NullWritable.get());
		}
	} 
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "union");
		
		job.setJarByClass(Union.class);
		
		job.setMapperClass(UnionMapper.class);
		job.setReducerClass(UnionReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
