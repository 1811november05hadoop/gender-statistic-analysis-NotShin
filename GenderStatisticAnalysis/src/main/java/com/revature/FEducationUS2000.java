package com.revature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FEducationUS2000Mapper;
import com.revature.reduce.FEducationUS2000AverageIncreaseReducer;
import com.revature.reduce.FEducationUS2000GrossPercentageReducer;

public class FEducationUS2000 {
	
	public static void main(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		if(args.length != 2) {
			System.err.println();
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "FEducationAverageIncreaseUS2000");
		
		job.setJarByClass(FEducationUS2000.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FEducationUS2000Mapper.class);
		job.setCombinerClass(FEducationUS2000GrossPercentageReducer.class);
		job.setReducerClass(FEducationUS2000AverageIncreaseReducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
