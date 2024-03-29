package com.atguigu.mr.group;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
	
	@Override
	protected void reduce(OrderBean key, Iterable<NullWritable> values,
			Reducer<OrderBean, NullWritable, OrderBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
//		for (NullWritable nullWritable : values) {
//			context.write(key, NullWritable.get());
//		}
		
		context.write(key, NullWritable.get());
	}
	
}
