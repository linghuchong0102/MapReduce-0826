package com.atguigu.mr.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 驱动类
 * 	1. 获取Job对象
 *  2. 关联jar 指定当前Job的驱动类
 *  3. 关联Mapper 和 Reducer
 *	4. 设置Map输出的key 和 value的类型
 *  5. 设置最终输出的key 和 value的类型
 *  6. 设置输入和输出路径
 *  7. 提交Job
 */
public class WordCountDriver {
	
	public static void main(String[] args) throws Exception {
		//FileAlreadyExistsException: Output directory file:/d:/output1 already exists
		args = new String[] {"d:/input/inputWord","d:/output3"};	
		
		//1. 获取Job对象
		Configuration conf  = new Configuration();
		Job job = Job.getInstance(conf);
		
		//2. 关联jar
		job.setJarByClass(WordCountDriver.class);
		
		//3. 关联 Mapper 和 Reducer
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//4. 设置Map输出key和value的类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//5. 设置最终输出的key 和value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//6. 设置输入和输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置Reduce的个数
		job.setNumReduceTasks(2);
		
		//设置Combiner
		job.setCombinerClass(WordCountReducer.class);
		
		//7. 提交Job
		
		boolean flag = job.waitForCompletion(true);
		
		System.exit(flag?0:1);
		
	}
}













