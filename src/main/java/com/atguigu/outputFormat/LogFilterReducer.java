package com.atguigu.outputFormat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LogFilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
		for (NullWritable nullWritable : values) {
			context.write(key, NullWritable.get());
		}
	
	}
}
