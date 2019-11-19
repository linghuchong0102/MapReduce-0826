package com.atguigu.mr.topn;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopNMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	TreeMap<FlowBean,Text>  maps = new TreeMap<>();
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		//获取一行数据   13470253144	180	180	360
		String line = value.toString();
		//切割数据
		String [] splits = line.split("\t");
		//封装key
		//FlowBean k = new FlowBean(Long.parseLong(splits[1]),Long.parseLong(splits[2]));
		FlowBean k = new FlowBean();
		k.setUpFlow(Long.parseLong(splits[1]));
		k.setDownFLow(Long.parseLong(splits[2]));
		k.setSumFlow(Long.parseLong(splits[3]));
		
		//封装value
		Text v  = new Text();
		v.set(splits[0]);
	
		//将当前的kv存储到TreeMap中
		maps.put(k, v);
		
		//控制TreeMap中的数量为10 
		if(maps.size() >10 ) {
			//移除最后一个
			maps.remove(maps.lastKey());
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, FlowBean, Text>.Context context)
			throws IOException, InterruptedException {
		//将TreeMap中的数据写入到Reduce中
		Set<Entry<FlowBean, Text>> entrySet = maps.entrySet();
		for (Entry<FlowBean, Text> entry : entrySet) {
			//计数器
			context.getCounter("Map TreeMap","TreeMap count").increment(1);
			//获取key  和  value
			FlowBean key = entry.getKey();
			Text value = entry.getValue();
			//写出
			context.write(key, value);
		}
	
	}
}
