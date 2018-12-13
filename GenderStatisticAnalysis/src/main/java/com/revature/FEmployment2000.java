package com.revature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.FEmployment2000Mapper;

public class FEmployment2000 {
	
	public static void main(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		if(args.length != 2) {
			System.err.println();
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "FEmployment2000");
		
		job.setJarByClass(FEmployment2000.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(FEmployment2000Mapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
