package com.atguigu.mr.myInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SmallFileMapper  extends Mapper<Text, BytesWritable, Text, BytesWritable>{
	
	@Override
	protected void map(Text key, BytesWritable value, Mapper<Text, BytesWritable, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, value);
	}
}
