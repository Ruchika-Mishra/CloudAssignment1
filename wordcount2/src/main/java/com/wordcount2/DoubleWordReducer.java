package com.wordcount2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


//reduces the redundant words to single word and sums up their counts
public class DoubleWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable doubleWordSum = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int count = 0;
		for (IntWritable value : values) {
			count += value.get();
	      }
		doubleWordSum.set(count);
		context.write(key, doubleWordSum);
	}
}
