package snnu.hadoop.intest;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JoinQuery {
	
	public static class JoinQueryMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text k = new Text();
		private Text v = new Text();
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			
			String[] filed = StringUtils.split(line, "\t");
			
			String id = filed[0];
			String name = filed[1];
			
			FileSplit inputSplit = (FileSplit)context.getInputSplit();
			String filename = inputSplit.getPath().getName();
			
			k.set(id);
			v.set(name + ":" + filename);
			
			context.write(k, v);
			
		}
	}
		
		public static class  JoinQueryReducer extends Reducer<Text, Text, Text, Text> {
			
			@Override
			protected void reduce(Text key, Iterable<Text> values,  Context context)
					throws IOException, InterruptedException {
				
				String leftFied = "";
				ArrayList<String> rightFilds = new ArrayList<>();
				for(Text value : values){
					if(StringUtils.split(value.toString(), ":")[1].contains("a.txt")){
						leftFied = StringUtils.split(value.toString(), ":")[0];
					}else{
						rightFilds.add(StringUtils.split(value.toString(), ":")[0]);
					}
				}
				
				for(String right : rightFilds){
					context.write(new Text(leftFied), new Text(right));
				}
			}
		}
		
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf);
			
			job.setJarByClass(JoinQuery.class);
			
			job.setMapperClass(JoinQueryMapper.class);
			job.setReducerClass(JoinQueryReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		}
}
