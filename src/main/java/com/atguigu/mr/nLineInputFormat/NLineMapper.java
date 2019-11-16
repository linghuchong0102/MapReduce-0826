package com.atguigu.mr.nLineInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NLineMapper extends Mapper<LongWritable, Text	, Text, IntWritable> {
	
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//获取一行
		String line = value.toString();
		
		//切割数据
		String[] split = line.split(" ");
		
		//迭代写出
		for (String string : split) {
			
			k.set(string);
			
			context.write(k, v);
		}
	}
}
