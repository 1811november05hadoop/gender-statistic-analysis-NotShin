package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class FGraduatesLessThan30Reducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	MultipleOutputs<IntWritable, Text> mos;
	public static Set<String> uniqueCountries = new TreeSet<>();
	
	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<>(context);
	}
	
	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		List<String> countries = new ArrayList<>();
		
		for(Text value: values) {
			countries.add(value.toString());
			uniqueCountries.add(value.toString());
		}
		
		Collections.sort(countries);
		
		mos.write("DateCountries", key, new Text(String.join(" | ", countries)));
	}
	
	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {
		for(String country: uniqueCountries) {
			mos.write("ListCountries", NullWritable.get(), new Text(country));
		}
		mos.close();
	}
}
