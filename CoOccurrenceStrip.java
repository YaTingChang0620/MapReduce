package co_occurrence;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import java.io.*;
import org.apache.hadoop.io.Writable;
import java.util.*;

public class CoOccurrenceStrip {
	
	public static class coOccurrenceStripMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			MapWritable map = new MapWritable();
			
			
			String[] words = value.toString().split(" ");
			int window = 3;
				
			for (int i = 0; i < words.length; i++) {
				String currWord = words[i];
				currWord = currWord.replaceAll("[“\\s\\p{Punct}]", "");
				
				for (int j = i - window; j < i + window + 1; j++) {
					
					if (i == j || j < 0) continue;
					if (j >= words.length) break;
					
					String windowWord_temp = words[j].replaceAll("[“\\s\\p{Punct}]", "");
					Text windowWord = new Text(windowWord_temp);
					
					if (map.containsKey(windowWord)) {
						int v = ((IntWritable)map.get(windowWord)).get();
						v++;
						map.put(new Text(windowWord), new IntWritable(v));
					} else {
						map.put(new Text(windowWord), new IntWritable(1));
					}
				}
				context.write(new Text(currWord), map);
				map.clear();
			}
		}
	}
	
	public static class coOccurrenceReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
		public void reduce(Text key, Iterable<MapWritable> value, Context context) throws IOException, InterruptedException{
			MapWritable map = new MapWritable();
			
			for (MapWritable v: value) {
				for (Map.Entry<Writable, Writable> m: v.entrySet()) {
					Text co_word = (Text)m.getKey();
					if (map.containsKey(co_word)) {
						IntWritable co_count = (IntWritable)m.getValue();
						int co_count_temp = co_count.get();
						co_count_temp++;
						map.put(co_word, new IntWritable(co_count_temp));
					} else {
						map.put(co_word, new IntWritable(1));
					}
				}
			}
			context.write(key, map);
			map.clear();
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "co-occurrence");
		job.setJarByClass(CoOccurrenceStrip.class);
		
		job.setMapperClass(coOccurrenceStripMapper.class);
		job.setReducerClass(coOccurrenceReducer.class);
		job.setCombinerClass(coOccurrenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?1:0);
	}
	
	

}
