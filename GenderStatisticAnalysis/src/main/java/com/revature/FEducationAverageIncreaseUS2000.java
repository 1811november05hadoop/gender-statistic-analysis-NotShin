package com.revature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FEducationAverageIncreaseUS2000Mapper;
import com.revature.reduce.FEducationAverageIncreaseUS2000Reducer;

public class FEducationAverageIncreaseUS2000 {
	
	public static void main(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		if(args.length != 2) {
			System.err.println();
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "FEducationAverageIncreaseUS2000");
		
		job.setJarByClass(FEducationAverageIncreaseUS2000.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FEducationAverageIncreaseUS2000Mapper.class);
		job.setReducerClass(FEducationAverageIncreaseUS2000Reducer.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}
}
