package com.atguigu.mr.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper :  继承Hadoop提供的Mapper类，重写map方法.
 * 
 * KV:
 * 输入的KV:
 * 	KEYIN : 输入的key的类型
 *  VALUEIN: 输入的value的类型
 * 输出的KV:
 *  KEYOUT: 输出的key的类型 
 *  VALUEOUT: 输出的value的类型
 *  
 * 输入数据:
 *  atguigu atguigu
	ss ss
	cls cls
	jiao
	banzhang
	xue
	hadoop
  
       输出数据:
 *  atguigu	2
	banzhang	1
	cls	2
	hadoop	1
	jiao	1
	ss	2
	xue	1
	
	
	当前WordCount输入和输出 KV分析:
	输入KEY: LongWritable  用于表示文件读取位置的偏移量. (记录文件读取到的位置，明确下次从哪里开始读取)
	输入VALUE: Text ,用于表示读取到的一行数据
	
	输出KEY:  Text ,用于表示当前分析到的key.(就是一个单词)
	输出VVALUE: IntWritable ,用于表示当前key对应的个数(当前就是1)

 *  
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	Text k = new Text();
	IntWritable v = new IntWritable(1);  
	
	@Override
	protected void map(LongWritable key, Text value, 
						Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		//1. 获取读取到的一行数据  atguigu atguigu
		String line = value.toString();
		//2. 切割数据
		String [] splits = line.split(" ");
		
		//3. 迭代数据，输出kv
		for (String split : splits) {
			
			k.set(split);
			//写出
			context.write(k, v);
		}
	}

}


















