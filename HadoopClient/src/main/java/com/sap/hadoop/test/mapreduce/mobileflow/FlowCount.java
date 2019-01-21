package com.sap.hadoop.test.mapreduce.mobileflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author I052090
 *
 */
public class FlowCount {
	 
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>  {
		@Override
		protected void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			String[] fields = line.split("\t");
			
			String phoneNbr = fields[0];
			
			long upFlow = Long.parseLong(fields[1]);
			long dFlow = Long.parseLong(fields[2]);
			
			context.write(new Text(phoneNbr), new FlowBean(upFlow, dFlow));
			
		}
	 }
	 
	 static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
		 @Override 
		 protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
			 long sumUpFlow = 0;
			 long sumDFlow = 0;
			 for (FlowBean fBean:values) {
				 sumUpFlow+=fBean.getUpFlow();
				 sumDFlow+=fBean.getdFlow();
			 }
			FlowBean resultBean = new FlowBean(sumUpFlow, sumDFlow);
			
			context.write(key, resultBean);
		 }
		 
		 
	 }
	 
	 /**
	  * 
	  * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	  */
	 public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "FlowCount");
			
			job.setJarByClass(FlowCount.class);
			
			job.setMapperClass(FlowCountMapper.class);
			
			job.setReducerClass(FlowCountReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlowBean.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowBean.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			boolean status = job.waitForCompletion(true);
			
			if (!status) {
				System.out.println("Job failed" );
			}
		 
	 }
}
