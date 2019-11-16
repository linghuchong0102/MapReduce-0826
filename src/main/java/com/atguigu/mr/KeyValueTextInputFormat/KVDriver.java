package com.atguigu.mr.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KVDriver {
	
	public static void main(String[] args)  throws Exception{
		
		args = new String [] {"d:/input/inputkv","d:/output1"};
		
		//1. 获取Job
		Configuration conf= new Configuration();
		
		//设置分隔符
		conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		
		Job job = Job.getInstance(conf);
		
		//设置使用keyValueTextInputFormat
		job.setInputFormatClass(KeyValueTextInputFormat.class);
	
		//2.关联jar
		job.setJarByClass(KVDriver.class);
		
		//3. 关联Mapper  Reducer
		job.setMapperClass(KVMapper.class);
		job.setReducerClass(KVReducer.class);
		
		//4. 设置Map输出的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5. 设置最终输出的key 和 value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//6. 设置输入和输出路径
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	
		
		//7.提交Job
		job.waitForCompletion(true);
	}
}
