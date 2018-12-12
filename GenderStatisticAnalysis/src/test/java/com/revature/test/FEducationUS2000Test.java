package com.revature.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.FEducationUS2000Mapper;
import com.revature.reduce.FEducationUS2000AverageIncreaseReducer;
import com.revature.reduce.FEducationUS2000GrossPercentageReducer;

public class FEducationUS2000Test {
	
	private MapDriver<LongWritable, Text, NullWritable, DoubleWritable> mapDriver;
	private ReduceDriver<NullWritable, DoubleWritable, NullWritable, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, NullWritable, DoubleWritable, NullWritable, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setup() {
		FEducationUS2000Mapper mapper = new FEducationUS2000Mapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		FEducationUS2000GrossPercentageReducer combiner = new FEducationUS2000GrossPercentageReducer();
		FEducationUS2000AverageIncreaseReducer reducer = new FEducationUS2000AverageIncreaseReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setCombiner(combiner);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(1), new Text("United States,USA,,SE.TER.ENRR.FE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,81.5,83.5,,,,,,,,,,85.5,,,,,")));
		
		mapDriver.withOutput(new Pair<NullWritable, DoubleWritable>(NullWritable.get(), new DoubleWritable(81.5)));
		mapDriver.withOutput(new Pair<NullWritable, DoubleWritable>(NullWritable.get(), new DoubleWritable(83.5)));
		mapDriver.withOutput(new Pair<NullWritable, DoubleWritable>(NullWritable.get(), new DoubleWritable(85.5)));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<DoubleWritable> values = new ArrayList<>();
		values.add(new DoubleWritable(2));
		values.add(new DoubleWritable(2));
		
		reduceDriver.withInput(new Pair<NullWritable, List<DoubleWritable>>(NullWritable.get(), values));
		
		reduceDriver.withOutput(new Pair<NullWritable, DoubleWritable>(NullWritable.get(), new DoubleWritable(2.0)));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(1), new Text("United States,USA,,SE.TER.ENRR.FE,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,81.5,83.5,,,,,,,,,,85.5,,,,,")));
		
		mapReduceDriver.withOutput(new Pair<NullWritable, DoubleWritable>(NullWritable.get(), new DoubleWritable(2.0)));
		
		mapReduceDriver.runTest();
	}
}
