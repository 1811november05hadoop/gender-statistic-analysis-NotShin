package com.revature.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.MEmployment2000Mapper;

public class MEmployment2000Test {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	@Before
	public void setup() {
		MEmployment2000Mapper mapper = new MEmployment2000Mapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new Pair<LongWritable, Text>(new LongWritable(1),
				new Text("United States,USA,,SL.TLF.TOTL.FE.ZS"
						+ ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"
						+ "20.1,20.2,20.3,20.4,20.5,20.0,21.0,20.9,20.8,"
						+ "20.7,20.6,20.5,22.0,21.5,21.7,19.8,19.9")));
			
		mapDriver.withOutput(new Pair<Text, Text>(new Text("Country                                           "), 
				new Text("2000-01 | 2001-02 | 2002-03 | 2003-04 | 2004-05 | 2005-06 | 2006-07 | 2007-08 | "
						+ "2008-09 | 2009-10 | 2010-11 | 2011-12 | 2012-13 | 2013-14 | 2014-15 | 2015-16")));
		mapDriver.withOutput(new Pair<Text, Text>(new Text("--------------------------------------------"
				+ "-------------------------------------------------------------------------------------"
				+ "------------------------------------------------------------------------------------"), 
				new Text("")));
		mapDriver.withOutput(new Pair<Text, Text>(new Text("United States                                     "), 
				new Text(" -0.10% |  -0.10% |  -0.10% |  -0.10% |   0.50% |  -1.00% |   0.10% |   0.10% |"
						+ "   0.10% |   0.10% |   0.10% |  -1.50% |   0.50% |  -0.20% |   1.90% |  -0.10%")));
		
		mapDriver.runTest();
	}
}
