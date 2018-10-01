package com.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.StringTokenizer;
import java.io.IOException;

//maps the given input file string to double words and count as key/value pairs
public class DoubleWordMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Text doubleWord = new Text();
	private final static IntWritable one = new IntWritable(1);
   
	@Override
	public void map(Object key, Text value, Context context) throws IOException,
			InterruptedException {

		String firstWord = "";
		String secondWord = "";
				
		StringTokenizer inputText = new StringTokenizer(value.toString());
		if (inputText.hasMoreTokens()) {
			firstWord = inputText.nextToken(); 
		}
		while (inputText.hasMoreTokens()) {
			 secondWord = inputText.nextToken();
			
			doubleWord.set(firstWord + " " + secondWord);
			context.write(doubleWord, one);
			firstWord = secondWord; 
		}
	}
}