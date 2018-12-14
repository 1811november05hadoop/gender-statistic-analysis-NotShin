package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FGraduatesLessThan30Mapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		if (value.toString().contains("SE.TER.CMPL.FE.ZS")) {
			
			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			// Clean the data
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}
			
			// 42 to values.length = 1998 to 2016
			for(int i = 42; i < values.length; i++) {
				if (!values[i].equals("") && Double.parseDouble(values[i]) < 30) {
					context.write(new IntWritable(1956 + i), new Text(values[0]));
				}
			}
		}
	}
}
