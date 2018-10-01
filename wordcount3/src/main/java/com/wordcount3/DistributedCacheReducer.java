package com.wordcount3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.Iterator;


public class DistributedCacheReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable totalCount = new IntWritable();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		Iterator<IntWritable> it = values.iterator();
		while (it.hasNext()) {
			wordCount = wordCount+it.next().get();
		}
		totalCount.set(wordCount);
		context.write(key, totalCount);
	}
}