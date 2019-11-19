package com.atguigu.mr.mapJoin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Text k = new Text();
	
	Map<String,String>  pdMap = new HashMap<>();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		//计数器
		context.getCounter("Map Join", "setup").increment(1);
		
		
		//获取到缓存文件
		URI [] cacheFiles = context.getCacheFiles();
		URI currentCacheFile = cacheFiles[0];
		System.out.println(currentCacheFile);
		//获取到文件系统对象
		FileSystem fs = FileSystem.get(context.getConfiguration());
		//获取输入流
		FSDataInputStream in = fs.open(new Path(currentCacheFile));
		//读取文件中的内容 ，按行读取
		BufferedReader  br = new BufferedReader(new InputStreamReader(in));
		String line ;
		while((line = br.readLine())!=null) {
			//切割数据    01	小米
			String [] splits = line.split("\t");
			pdMap.put(splits[0], splits[1]);
		}
	}	
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter("Map Join", "map").increment(1);
		
		//读取order.txt中的一行数据   1006	 03	 6
		String line = value.toString();
		//切割
		String [] splits = line.split("\t");
		
		//通过产品id 到pdMap中获取 产品名
		String productName  = pdMap.get(splits[1]);
		
		//拼接数据
		
		String resultLine =  splits[0] + "\t" + productName + "\t" + splits[2];
		
		//封装key
		k.set(resultLine);
		
		//写出
		context.write(k, NullWritable.get());
	
	}
}
