package com.atguigu.mr.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  自定义Combiner需要继承Reducer类. Combiner是运行在Map阶段。
 *
 */
public class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		
		int sum = 0 ;
		
		for (IntWritable intWritable : values) {
			sum += intWritable.get();
		}
		
		v.set(sum);
		
		context.write(key, v);
	}
	
}
