package com.atguigu.mr.reduceJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceJoinDriver {
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
	    job.setJarByClass(ReduceJoinDriver.class);
	
	    job.setMapperClass(ReduceJoinMapper.class);
	    job.setReducerClass(ReduceJoinReducer.class);
	
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(OrderBean.class);
	
	    job.setOutputKeyClass(OrderBean.class);
	    job.setOutputValueClass(NullWritable.class);
	
	    FileInputFormat.setInputPaths(job, new Path("d:/input/inputtable"));
	    FileOutputFormat.setOutputPath(job, new Path("d:/output2"));
	
	    boolean b = job.waitForCompletion(true);
	
	    System.exit(b ? 0 : 1);


	}
}
