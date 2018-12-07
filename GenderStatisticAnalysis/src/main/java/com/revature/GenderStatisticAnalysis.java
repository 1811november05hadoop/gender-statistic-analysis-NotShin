package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.YearMapper;
import com.revature.reduce.YearReducer;

//hadoop jar target/GenderStatisticAnalysis.jar com.revature.GenderStatisticAnalysis HData/gender-stats-data/Gender_StatsData.csv output-mapper-test

public class GenderStatisticAnalysis {
	
	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out.printf(
					"Usage: GenderStatisticAnalysis <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(GenderStatisticAnalysis.class);
		job.setJobName("Gender Statistic Analysis");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(YearMapper.class);
		job.setReducerClass(YearReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
