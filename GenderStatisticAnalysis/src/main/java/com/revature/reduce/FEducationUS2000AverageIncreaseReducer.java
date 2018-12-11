package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FEducationUS2000AverageIncreaseReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {

	@Override
	public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		double sum = 0;
		int count = 0;
		
		for(DoubleWritable value: values) {
			sum += value.get();
			count += 1;
		}
		
		context.write(NullWritable.get(), new DoubleWritable(sum / count));
	}
}
