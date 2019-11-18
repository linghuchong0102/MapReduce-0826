package com.atguigu.mr.group;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//获取一行   10000001	Pdt_01	222.8
		String line = value.toString();
		//切割
		String [] splits = line.split("\t");
		
		//封装Key
		OrderBean ob = new OrderBean();
		ob.setOrderId(Integer.parseInt(splits[0]));
		ob.setPrice(Double.parseDouble(splits[2]));
		
		context.write(ob, NullWritable.get());
	
	}
}
