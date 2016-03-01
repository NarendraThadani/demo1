package org.irobin.hadoop.demo2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateMin {
	
	public static class AggregateMapper extends Mapper<Object, Text, Text, Text> {
	
	
		@Override		
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			
			String year = tokenizer.nextToken();
			tokenizer.nextToken();
			tokenizer.nextToken();
			String delta = tokenizer.nextToken();
			context.write(new Text("Summary"), new Text( year + "_" + delta));
			
			
		}
		
	}
	public static class AggregateReducer extends Reducer<Text, Text, Text, FloatWritable> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {
		

			String compositeString;
			String []compositeStringArray;
			Text tempYear;
			long tempValue;
			long min;
			Text minYear;
			
			minYear = new Text("NULL");
			min = Long.MAX_VALUE;
			for (Text text : values) {
				compositeString = text.toString();
				compositeStringArray = compositeString.split("_");
				
				tempYear = new Text (compositeStringArray[0]);
				tempValue = new Long(compositeStringArray[1]   ).longValue();
				
				if(tempValue < min){
					
					min = tempValue ;
					minYear = tempYear;
				}
				
				
			}
			
			Text keyText = new Text ("min(" + minYear.toString() + "): ");
			context.write(keyText, new FloatWritable(min));
			
		}
		
		
	}
	

}
