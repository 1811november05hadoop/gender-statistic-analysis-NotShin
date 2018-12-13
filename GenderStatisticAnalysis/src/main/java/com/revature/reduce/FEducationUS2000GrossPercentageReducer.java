package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class FEducationUS2000GrossPercentageReducer extends Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
	
	@Override
	public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) 
			throws IOException, InterruptedException {
		
		DoubleWritable grossPercentage = new DoubleWritable(0);
		List<Double> yearlyIncrease = new ArrayList<>();
		
		for(DoubleWritable value: values) {
			if(grossPercentage.compareTo(new DoubleWritable(0)) != 0) {
				yearlyIncrease.add(new Double(grossPercentage.get() - value.get()));
				grossPercentage.set(value.get());
			} else {
				grossPercentage.set(value.get());
			}
		}
		
		for(Double val : yearlyIncrease) {
			context.write(NullWritable.get(), new DoubleWritable(val.doubleValue()));
		}
	}
}
