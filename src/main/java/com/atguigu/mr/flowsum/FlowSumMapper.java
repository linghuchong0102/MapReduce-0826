package com.atguigu.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper  extends Mapper<LongWritable, Text, Text, FlowBean >{
	
	Text k = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		/**
		 * 一行数据 :
		 *  7 	13560436666	120.196.100.99		1116		 954			200
			id	手机号码		网络ip			      上行流量                     下行流量                   网络状态码
		 */
		
		//1.获取到一行数据
		String line = value.toString();
		//2.切割数据
		String [] splits = line.split("\t");
		//3.提取手机号
		String phonenum =  splits[1];
		//4.提取上行  下行 流量
		String upFlow = splits[splits.length-3];
		String downFlow = splits[splits.length-2];
		
		//5. 封装FlowBean对象
		FlowBean fb = new FlowBean(Long.parseLong(upFlow),Long.parseLong(downFlow));
		//6.写出
		k.set(phonenum);
		context.write(k,fb);
	}
}
