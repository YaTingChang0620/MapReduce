package MapSideJoin;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.join.CompositeInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.commons.lang3.StringUtils;

public class MapSideJoin {
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("mapred.join.expr", 
				CompositeInputFormat.compose("inner", KeyValueTextInputFormat.class, new Path(args[0]), new Path(args[1])));
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		
		Job job = Job.getInstance(conf, "map-side join");
		job.setJarByClass(MapSideJoin.class);
		
		job.setMapperClass(MapSideJoinMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(CompositeInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true)?1:0);
	}
	
	public static class MapSideJoinMapper extends Mapper<Text, TupleWritable, Text, Text> {
		public void map(Text key, TupleWritable value, Context context) throws IOException, InterruptedException{
			//context.write(key, (Text)value.get(0));
			context.write(key, new Text(StringUtils.join(value.get(0), new Text(","), value.get(1))));
		}
	}

}
