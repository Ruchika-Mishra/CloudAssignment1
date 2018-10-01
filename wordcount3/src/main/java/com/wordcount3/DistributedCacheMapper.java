package com.wordcount3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.io.FileUtils;

public class DistributedCacheMapper extends Mapper<Object, Text, Text, IntWritable> {
	List<String> fileWordList = new ArrayList<String>(); 
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	StringTokenizer lines;

	@Override
	public void map(Object key, Text value, Context context) throws IOException,
			InterruptedException {
		lines = new StringTokenizer(value.toString());

		String nextToken = "";
		while (lines.hasMoreTokens()) {
			// get next token
			nextToken = lines.nextToken();
			
			// generate key,value pair if token exists in the list of words from cached file
			if (fileWordList.contains(nextToken)) {
				word.set(nextToken);
				context.write(word, one);
			}
		}
	}
	
	@Override
	protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {		
		processFile();
		super.setup(context);
	}

	
	/**
	 * processes file to add words in list, from cached file
    **/
	private void processFile() throws IOException {
		String distributed_words = FileUtils.readFileToString(new File("./cache"));
		lines = new StringTokenizer(distributed_words);
			
				while (lines.hasMoreTokens()) {
					fileWordList.add(lines.nextToken());
				}
	}
}
