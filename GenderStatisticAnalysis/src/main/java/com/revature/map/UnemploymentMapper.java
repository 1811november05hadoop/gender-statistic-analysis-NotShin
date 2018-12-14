package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.writable.DoubleArrayWritable;

public class UnemploymentMapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();
		
		/* SL.UEM.ADVN.FE.ZS
		 * Unemployment with advanced education, female
		 * (% of female labor force with advanced education)
		 * 
		 * SL.UEM.ADVN.MA.ZS
		 * Unemployment with advanced education, male
		 * (% of male labor force with advanced education) 
		 * 
		 * SL.UEM.TOTL.FE.NE.ZS
		 * Unemployment, female
		 * (% of female labor force) (national estimate)
		 * 
		 * SL.UEM.TOTL.MA.NE.ZS
		 * Unemployment, male
		 * (% of male labor force) (national estimate)
		 */
		if(line.contains("United States") && 
				(line.contains("SL.UEM.ADVN.FE.ZS") | 
						line.contains("SL.UEM.ADVN.MA.ZS") | 
						line.contains("SL.UEM.TOTL.FE.NE.ZS") |
						line.contains("SL.UEM.TOTL.MA.NE.ZS"))) {

			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

			// Clean the data
			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}

			String indicatorCode = values[3];
			DoubleArrayWritable doubleArrayWritable = new DoubleArrayWritable();
			DoubleWritable[] data = new DoubleWritable[32];;
			
			// 29 to values.length = 1985 to 2016
			for(int i = 29; i < values.length; i++) {
				data[i-29] = new DoubleWritable(Double.parseDouble(values[i]));
			}
			
			doubleArrayWritable.set(data);
			context.write(new Text(indicatorCode), doubleArrayWritable);
		}
	}
}
