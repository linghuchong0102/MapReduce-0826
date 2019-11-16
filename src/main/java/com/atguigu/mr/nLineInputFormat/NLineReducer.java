package com.atguigu.mr.nLineInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum  = 0 ; 
		
		for (IntWritable intWritable : values) {
			sum +=intWritable.get();
		}
		
		v.set(sum);
		context.write(key, v);
	
	}
}
