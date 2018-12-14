package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FEducationUS2000Mapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if(line.contains("United States") && line.contains("SE.TER.ENRR.FE")) {
			
			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			// Clean the data
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}
			
			// 44 to values.length = 2000 to 2016
			for(int i = 44; i < values.length; i++) {
				if(!values[i].equals("")) {
					context.write(NullWritable.get(), new DoubleWritable(Double.parseDouble(values[i])));
				}
			}
		}
	}
}
