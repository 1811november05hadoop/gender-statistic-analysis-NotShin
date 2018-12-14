package com.revature.map;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FEmployment2000Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void setup(Context context) 
			throws IOException, InterruptedException {
		
		// Write headers to context
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
		
		// Stores the inputs received from CSV file
		List<Double> percentFemaleLaborForce = new ArrayList<>();
		
		// Stores the difference in years as percent change per year
		List<String> percentChangeLaborForce = new ArrayList<>();

		if(line.contains("SL.TLF.TOTL.FE.ZS")) {

			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			// Clean the data
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}
			
			// 44 to values.length = 2000 to 2016
			for(int i = 44; i < values.length; i++) {
				if(!values[i].equals("")) {
					percentFemaleLaborForce.add(Double.parseDouble(values[i]));
				}
			}
			
			// Ensure the dataset for a country is complete
			if(percentFemaleLaborForce.size() == 17) {
				for(int i = 1; i < 17; i++) {
					// Calculate the difference in years to
					// determine percent change in labor force
					// each year
					percentChangeLaborForce.add(String.format("%7s", String.format("%.2f%%", 
							percentFemaleLaborForce.get(i).doubleValue() 
							- percentFemaleLaborForce.get(i - 1).doubleValue())));
				}
				
				context.write(new Text(String.format("%-50s", values[0])), 
						new Text(String.join(" | ", percentChangeLaborForce)));
			}
		}
	}
}
