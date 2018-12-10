package com.revature;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.revature.map.YearMapper;
import com.revature.reduce.YearReducer;

public class GenderStatisticAnalysis {
	
	public static void main(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		if (args.length != 2) {
			System.out.printf(
					"Usage: GenderStatisticAnalysis <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(GenderStatisticAnalysis.class);
		job.setJobName("Gender Statistic Analysis");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(YearMapper.class);
		job.setReducerClass(YearReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		MultipleOutputs.addNamedOutput(job, "DateCountries", TextOutputFormat.class, IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "ListCountries", TextOutputFormat.class, NullWritable.class, Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
