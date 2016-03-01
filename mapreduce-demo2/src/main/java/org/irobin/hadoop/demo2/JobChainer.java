package org.irobin.hadoop.demo2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobChainer {
	public static void main(String[] args) throws Exception {

		final String OUTPUT_PATH = "/test/intermediate_output";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Find Min");

		job.setJarByClass(AggregateMin.class);
		job.setMapperClass(AggregateMin.AggregateMapper.class);
		//job.setCombinerClass(AggregateMin.AggregateReducer.class);
		job.setReducerClass(AggregateMin.AggregateReducer.class);
				
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		
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
	
	}

}
