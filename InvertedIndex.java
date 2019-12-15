import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class InvertedIndex {
	
	
	public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			// get the file name
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				String token = st.nextToken();
				token = token.replaceAll("[\\s\\p{Punct}”]", "");
				context.write(new Text(token), new Text(fileName));
			}
		}

	}
	
	public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException{
			Map<String, Integer> map = new HashMap<String, Integer>();
			for (Text v: value) {
				if (!map.containsKey(v.toString())) {
					map.put(v.toString(), 1);
				} else {
					int count = map.get(v.toString());
					map.put(v.toString(), ++count);
				}
			}
			context.write(key, new Text(map.toString()));
		}

	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "inverted index");

		job.setJarByClass(InvertedIndex.class);
		
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}