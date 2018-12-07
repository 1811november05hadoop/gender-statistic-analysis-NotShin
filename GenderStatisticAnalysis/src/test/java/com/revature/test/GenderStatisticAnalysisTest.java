package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.YearMapper;
import com.revature.reduce.YearReducer;

public class GenderStatisticAnalysisTest {
	
	private MapDriver<LongWritable, Text, IntWritable, Text> mapDriver;
	private ReduceDriver<IntWritable, Text, IntWritable, Text> reduceDriver;
	private MapReduceDriver<LongWritable, Text, IntWritable, Text, IntWritable, Text> mapReduceDriver;
	
	@Before
	public void setup() {
		YearMapper mapper = new YearMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		YearReducer reducer = new YearReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() {
		mapDriver.withInput(new LongWritable(1), new Text("United States,USA,,SE.TER.CMPL.FE.ZS,20,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,20,20"));
		
		mapDriver.withOutput(new IntWritable(1960), new Text("United States"));
		mapDriver.withOutput(new IntWritable(2015), new Text("United States"));
		mapDriver.withOutput(new IntWritable(2016), new Text("United States"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() {
		List<Text> values = new ArrayList<>();
		values.add(new Text("United States"));
		
		reduceDriver.withInput(new IntWritable(1970), values);
		
		reduceDriver.withOutput(new IntWritable(1970), new Text("United States"));
		
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReducer() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("United States,USA,,SE.TER.CMPL.FE.ZS,20,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,20,20"));
		
		mapReduceDriver.withOutput(new IntWritable(1960), new Text("United States"));
		mapReduceDriver.withOutput(new IntWritable(2015), new Text("United States"));
		mapReduceDriver.withOutput(new IntWritable(2016), new Text("United States"));
		
		mapReduceDriver.runTest();
	}
}
