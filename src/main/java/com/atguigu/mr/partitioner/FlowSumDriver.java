package com.atguigu.mr.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class FlowSumDriver {
	
	public static void main(String[] args)  throws Exception{
		
		args = new String[] {"D:/input/inputflow","d:/output7"};
		
		//1. 获取Job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2. 关联jar
		job.setJarByClass(FlowSumDriver.class);
		
		//3. 关联Mapper  Reducer
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		//4. 设置map输出的key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//5. 设置 最终输出的key和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//6. 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		//设置分区器
		job.setPartitionerClass(PhoneNumberPartitioner.class);
		//设置reduce的个数
		job.setNumReduceTasks(5);
		
		//7.提交Job
		job.waitForCompletion(true);
		
	}
}
