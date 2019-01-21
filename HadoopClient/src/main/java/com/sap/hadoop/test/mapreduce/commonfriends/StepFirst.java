package com.sap.hadoop.test.mapreduce.commonfriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepFirst {
	static class FirstMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split(":");
			String user = arr[0];
			String friends = arr[1];

			for (String friend : friends.split(",")) {
				context.write(new Text(friend), new Text(user));
			}
		}
	}
	
	static class FirstReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text friend, Iterable<Text> users, Context context) throws IOException, InterruptedException {
			StringBuffer buf = new StringBuffer();
			for (Text user: users) {
				buf.append(user).append(",");
			}
			context.write(new Text(friend), new Text(buf.toString()));
		}	
	}
	
	/**
	 * 
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "StepFirst");
		
		job.setJarByClass(StepFirst.class);
		
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		
	}
}
