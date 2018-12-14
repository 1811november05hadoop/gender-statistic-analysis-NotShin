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

		if(line.contains("United States") && 
				(line.contains("SL.UEM.ADVN.FE.ZS") | 
						line.contains("SL.UEM.ADVN.MA.ZS") | 
						line.contains("SL.UEM.TOTL.FE.NE.ZS") |
						line.contains("SL.UEM.TOTL.MA.NE.ZS"))) {

			String[] values = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

			for(int i = 0; i < values.length; i++) {
				values[i] = values[i].replace("\"", "").trim();
			}

			String indicatorCode = values[3];
			DoubleArrayWritable doubleArrayWritable = new DoubleArrayWritable();
			DoubleWritable[] data = new DoubleWritable[32];;
			
			// 29 = 1985
			for(int i = 29; i < values.length; i++) {
				data[i-29] = new DoubleWritable(Double.parseDouble(values[i]));
			}
			
			doubleArrayWritable.set(data);
			context.write(new Text(indicatorCode), doubleArrayWritable);
		}
	}
}
