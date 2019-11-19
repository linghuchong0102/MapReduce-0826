package com.atguigu.mr.mapJoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class MapJoinDriver {
	
	public static void main(String[] args)  throws Exception{
		
		args=new String[] {"d:/input/inputtable2","d:/output2"};
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapJoinDriver.class);
		
		job.setMapperClass(MapJoinMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		//缓存小文件
		job.addCacheFile(new URI("file:///d:/input/inputtable/pd.txt"));
		
		//设置reduce的个数为0
		job.setNumReduceTasks(0);
		
		job.waitForCompletion(true);
		
	}
}
