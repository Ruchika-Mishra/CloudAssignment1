package com.wordcount2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;

public class DoubleWordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//represents a MapReduce job configuration
		Job job = Job.getInstance(new Configuration());
		
		FileInputFormat.setInputPaths(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job, new Path(args[1])); 
		
		job.setJarByClass(DoubleWordCount.class); 
		
		job.setJobName("DoubleCount");
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 

		//sets the mapper, combiner and reducer class
		job.setMapperClass(DoubleWordMapper.class); 
		job.setCombinerClass(DoubleWordReducer.class);
		job.setReducerClass(DoubleWordReducer.class);

		job.setInputFormatClass(TextInputFormat.class);  
		job.setOutputFormatClass(TextOutputFormat.class); 
		
		//set the number of reducers to use
		job.setNumReduceTasks(7);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}