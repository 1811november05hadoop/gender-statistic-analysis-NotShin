package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.writable.DoubleArrayWritable;

public class UnemploymentReducer extends Reducer<Text, DoubleArrayWritable, NullWritable, Text> {
	public static List<Double> femaleAdvancedUnemployment = new ArrayList<>();
	public static List<Double> maleAdvancedUnemployment = new ArrayList<>();
	public static List<Double> femaleUnemployment = new ArrayList<>();
	public static List<Double> maleUnemployment = new ArrayList<>();

	@Override
	public void setup(Context context) 
			throws IOException, InterruptedException{
		List<String> headers = new ArrayList<>();
		
		for(int i = 1985; i <= 2016; i++) {
			headers.add(" " + Integer.toString(i) + " ");
		}
		
		context.write(NullWritable.get(), new Text(String.join(" | ", headers)));
	}

	@Override
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context) 
			throws IOException, InterruptedException {


		for(DoubleArrayWritable doubleArrayWritable: values) {
			Writable[] temp = new DoubleWritable[32];
			temp = doubleArrayWritable.get();
			for(int i = 0; i < 32; i++) {
				if(key.toString().equals("SL.UEM.ADVN.FE.ZS")) {
					femaleAdvancedUnemployment.add(Double.parseDouble(temp[i].toString()));
				} else if(key.toString().equals("SL.UEM.ADVN.MA.ZS")) {
					maleAdvancedUnemployment.add(Double.parseDouble(temp[i].toString()));
				} else if(key.toString().equals("SL.UEM.TOTL.FE.NE.ZS")) {
					femaleUnemployment.add(Double.parseDouble(temp[i].toString()));
				} else if(key.toString().equals("SL.UEM.TOTL.MA.NE.ZS")) {
					maleUnemployment.add(Double.parseDouble(temp[i].toString()));
				}
			}
		}
	}

	@Override
	public void cleanup(Context context) 
			throws IOException, InterruptedException {

		List<String> percentAdvancedUnemployment = new ArrayList<>();

		for(int i = 0; i < femaleAdvancedUnemployment.size(); i++) {
			percentAdvancedUnemployment.add(String.format("%6s", String.format("%.2f%%", 
					(femaleAdvancedUnemployment.get(i)
							+ maleAdvancedUnemployment.get(i))
							/ (femaleUnemployment.get(i)
									+ maleUnemployment.get(i))
									* 100)));
		}
		
		context.write(NullWritable.get(), new Text(String.join(" | ", percentAdvancedUnemployment)));
	}
}
