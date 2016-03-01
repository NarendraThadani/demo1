package org.irobin.hadoop.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobChainer {

	public static void main(String[] args) throws Exception {

		final String OUTPUT_PATH = "/test/intermediate_output";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Character Count");

		job.setJarByClass(CharCount.class);
		job.setMapperClass(CharCount.TokenizerMapper.class);
		job.setCombinerClass(CharCount.IntSumReducer.class);
		job.setReducerClass(CharCount.IntSumReducer.class);
		// sort keys in descending order
		
				job.setSortComparatorClass(CharCount.CharComparator.class);
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[1]));
		// delete intermediate output folder if it already exists
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(OUTPUT_PATH))) {
			fs.delete(new Path(OUTPUT_PATH), true);
		}

		// set the output path of this job
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		// wait for the job to finish
		job.waitForCompletion(true);

		/*
		// configure the second mapreduce job (CharCountSorter)
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "char count sorter");
		job2.setJarByClass(CharCountSorter.class);
		job2.setMapperClass(CharCountSorter.SimpleMapper.class);
		job2.setReducerClass(CharCountSorter.SimpleReducer.class);
		// sort keys in descending order
		job2.setSortComparatorClass(CharCountSorter.IntComparator.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		// take in input from the intermediate folder (the output of the 1st
		// mapreduce)
		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
		// output the results of the second mapreduce to a user specified path
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		// wait for the job to finish and exit
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		*/
	}

}
