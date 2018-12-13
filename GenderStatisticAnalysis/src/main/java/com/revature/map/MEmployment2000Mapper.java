package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MEmployment2000Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		context.write(new Text(String.format("%-50s", "Country")), new Text("2000-01 "
				+ "| 2001-02 | 2002-03 | 2003-04 | 2004-05 | 2005-06 | 2006-07 "
				+ "| 2007-08 | 2008-09 | 2009-10 | 2010-11 | 2011-12 | 2012-13 "
				+ "| 2013-14 | 2014-15 | 2015-16"));
		context.write(new Text(String.join("", Collections.nCopies(213, "-"))), new Text(""));
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		List<Double> percentMaleLaborForce = new ArrayList<>();
		List<String> percentChangeLaborForce = new ArrayList<>();

		if(line.contains("SL.TLF.TOTL.FE.ZS")) {

			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}
			
			for(int i = 44; i < values.length; i++) {
				if(!values[i].equals("")) {
					percentMaleLaborForce.add(100 - Double.parseDouble(values[i]));
				}
			}
			
			if(percentMaleLaborForce.size() == 17) {
				for(int i = 1; i < 17; i++) {
					percentChangeLaborForce.add(String.format("%7s", String.format("%.2f%%", 
							percentMaleLaborForce.get(i).doubleValue() 
							- percentMaleLaborForce.get(i - 1).doubleValue())));
				}
				
				context.write(new Text(String.format("%-50s", values[0])), 
						new Text(String.join(" | ", percentChangeLaborForce)));
			}
		}
	}
}
