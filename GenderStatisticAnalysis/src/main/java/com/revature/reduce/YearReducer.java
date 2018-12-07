package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class YearReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		List<String> countries = new ArrayList<>();
		
		for(Text value: values) {
			countries.add(value.toString());
		}
		
		Collections.sort(countries);
		
		context.write(key, new Text(String.join(" | ", countries)));
	}
}
