package com.revature.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.revature.map.YearMapper;
import com.revature.reduce.YearReducer;

@RunWith(PowerMockRunner.class)
@PrepareForTest(YearReducer.class)
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
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("United States,USA,,SE.TER.CMPL.FE.ZS,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,20,,,,,,,,,,,,,,,,,20,20"));
		
		mapDriver.withOutput(new IntWritable(1998), new Text("United States"));
		mapDriver.withOutput(new IntWritable(2015), new Text("United States"));
		mapDriver.withOutput(new IntWritable(2016), new Text("United States"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<>();
		values.add(new Text("United States"));
		
		reduceDriver.withInput(new IntWritable(1970), values);
		
		reduceDriver.withMultiOutput("DateCountries", new IntWritable(1970), new Text("United States"));
		reduceDriver.withMultiOutput("ListCountries", NullWritable.get(), new Text("United States"));
		
		reduceDriver.runTest();
	}
	
//	@Test
//	public void testMapReducer() throws IOException {
//		mapReduceDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(1), new Text("United States,USA,,SE.TER.CMPL.FE.ZS,20,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,20,20")));
//
//		mapReduceDriver.withPathOutput(new Pair<IntWritable, Text>(new IntWritable(1960), new Text("United States")), "DateCountries");
//		mapReduceDriver.withPathOutput(new Pair<IntWritable, Text>(new IntWritable(2015), new Text("United States")), "DateCountries");
//		mapReduceDriver.withPathOutput(new Pair<IntWritable, Text>(new IntWritable(2016), new Text("United States")), "DateCountries");
//		
//		mapReduceDriver.withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("United States")), "ListCountries");
//		mapReduceDriver.withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("United States")), "ListCountries");
//		mapReduceDriver.withPathOutput(new Pair<NullWritable, Text>(NullWritable.get(), new Text("United States")), "ListCountries");
//		
//		mapReduceDriver.runTest();
//	}
}
