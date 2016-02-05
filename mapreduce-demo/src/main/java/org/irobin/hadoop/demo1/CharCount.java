package org.irobin.hadoop.demo1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class CharCount {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>  {
		
		private static final IntWritable one = new IntWritable(1);
		private Text singleChar = new Text();
		
		@Override		
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String []chars;
			
			chars= value.toString().split("");
			
			for (String string : chars) {
				singleChar.set(string);
				context.write(singleChar, one);
			}
			
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result  = new IntWritable();
		
		@Override		
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			int sum=0;
			
			for (IntWritable intWritable : values) {
				sum+= intWritable.get();
			}
			result.set(sum);
			
			context.write(key, result);
		}
	}
	
}
