package com.atguigu.mr.reduceJoin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
	
	private String fileName; 
	
	private Text  k  = new Text();
	
	private OrderBean v = new OrderBean();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, OrderBean>.Context context)
			throws IOException, InterruptedException {
		//获取当前的切片对象
		InputSplit split = context.getInputSplit();
		FileSplit fileSplit = (FileSplit)split ;
		
		//获取切片对应的文件的名字 
		 fileName = fileSplit.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, OrderBean>.Context context)
			throws IOException, InterruptedException {
		//1. 获取一条数据
		// 1001	01	1
		// 01	小米
		String line = value.toString();
		
		//2. 切割数据
		String [] splits = line.split("\t");
		if(fileName.contains("order")) {
			//订单表
			//封装key
			k.set(splits[1]);
			//封装value
			v.setOrderId(splits[0]);
			v.setAmount(Integer.parseInt(splits[2]));
			v.setProductId(splits[1]);
			v.setTitle("order");
			v.setProductName("");
		}else {
			//产品表
			//封装key
			k.set(splits[0]);
			//封装value
			v.setProductName(splits[1]);
			v.setProductId(splits[0]);
			v.setTitle("pd");
			v.setOrderId("");
			v.setAmount(0);
		}
		
		//写出
		context.write(k, v);	
	}

}
