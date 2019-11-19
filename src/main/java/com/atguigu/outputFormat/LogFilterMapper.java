package com.atguigu.outputFormat;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogFilterMapper  extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String log = value.toString() +"\r\n";
		k.set(log);

		context.write(k, NullWritable.get());
	
	}
}
