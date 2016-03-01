package org.irobin.hadoop.demo2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class AggregateMinDecade {

	public static class AggregateMapper extends Mapper<Object, Text, Text, Text> {

		enum Aggregation {  NONNUMERIC_YEAR};
		
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			StringTokenizer tokenizer = new StringTokenizer(value.toString());

			String year = tokenizer.nextToken();
			tokenizer.nextToken();
			tokenizer.nextToken();
			String delta = tokenizer.nextToken();

			try {
				int tempYear;
				tempYear = Integer.parseInt(year);
				context.write(new Text("decade" + "_" + (tempYear - (tempYear % 10))), new Text(year + "_" + delta));
			} catch (NumberFormatException nfe) {
				System.err.println("Error: Non Numeric Value for Year:" + year);
				context.setStatus("Detected Possibly incorrect Record: See Logs.");
				context.getCounter(Aggregation.NONNUMERIC_YEAR).increment(1);
			}
			

		}

	}

	public static class AggregateReducer extends Reducer<Text, Text, Text, FloatWritable> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, FloatWritable>.Context context)
				throws IOException, InterruptedException {

			String compositeString;
			String[] compositeStringArray;
			Text tempYear;
			long tempValue;
			long min;
			Text minYear;

			minYear = new Text("NULL");
			min = Long.MAX_VALUE;
			for (Text text : values) {
				compositeString = text.toString();
				compositeStringArray = compositeString.split("_");

				tempYear = new Text(compositeStringArray[0]);
				try {
					tempValue = new Long(compositeStringArray[1]).longValue();

					if (tempValue < min) {

						min = tempValue;
						minYear = tempYear;
					}
				} catch (NumberFormatException nfe) {
					System.err.println("Error: Invalid Number " + compositeStringArray[0] + " : " + compositeStringArray[1]);

				}

			}

			Text keyText = new Text("min(" + key.toString() + "_" + minYear.toString() + "): ");
			context.write(keyText, new FloatWritable(min));

		}

	}

}
