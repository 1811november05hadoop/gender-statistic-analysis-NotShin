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
			
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}
			
			for(int i = 32; i < values.length; i++) {
				if (!values[i].equals("") && Double.parseDouble(values[i]) < 30) {
					context.write(new IntWritable(1956 + i), new Text(values[0]));
				}
			}
		}
	}
}
