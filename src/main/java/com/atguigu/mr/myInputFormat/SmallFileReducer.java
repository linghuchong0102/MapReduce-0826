package com.atguigu.mr.myInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SmallFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Reducer<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
		
		context.write(key, values.iterator().next());
	
	}

}
