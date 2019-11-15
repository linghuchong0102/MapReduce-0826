package com.atguigu.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer: 继承Hadoop提供的Reducer类，重写reduce方法。
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		//一组kv: 
		// <atguigu,1> <atguigu,1>
		int sum = 0 ;
		//迭代 values，进行累加
		for (IntWritable value : values) {
			sum += value.get();
		}
		
		//sum = 2 
		v.set(sum);
		//写出
		
		context.write(key, v);
	
	}
}
