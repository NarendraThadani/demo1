package org.irobin.hadoop.demo1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCount {

	public static class CharComparator extends WritableComparator {

		public CharComparator() {

			super(Text.class,true);

		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(org.apache.hadoop.io.WritableComparable a, org.apache.hadoop.io.WritableComparable b) {
			return -1 * super.compare(a, b);
		};

		/*@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try
			{
				return -1 * super.compare(b1, s1, l1, b2, s2, l2);
			}
			catch(NullPointerException e)
			{
				return 0;
			}
		}
		@Override
		public int compare(Object a, Object b) {
 
			return  -1 * super.compare(a, b);
		}*/
	}

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		private static final IntWritable one = new IntWritable(1);
		private Text singleChar = new Text();

		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] chars;

			chars = value.toString().split("");

			for (String string : chars) {
				singleChar.set(string);
				context.write(singleChar, one);
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
						throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable intWritable : values) {
				sum += intWritable.get();
			}
			result.set(sum);

			context.write(key, result);
		}
	}

}
