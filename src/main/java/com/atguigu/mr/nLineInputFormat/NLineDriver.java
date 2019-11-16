package com.atguigu.mr.nLineInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {
	
	public static void main(String[] args) throws Exception {
		
		args = new String[] {"d:/input/inputnline" ,"d:/output1"};
		
		//1. 获取Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2. 关联jar
		job.setJarByClass(NLineDriver.class);
		
		//3. 关联Mapper  和 Reducer
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);
		
		//4.设置Mapper输出的key 和  value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5. 设置最终输出的key  和 value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//6.设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置使用NlineInputFormat
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setNumLinesPerSplit(job, 3);
		
		//7.提交Job
		job.waitForCompletion(true);
	}
}
