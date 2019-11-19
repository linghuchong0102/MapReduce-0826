package com.atguigu.outputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * 自定义RecordWriter 需要继承 RecordWriter
 * 
 * 需求: 将log.txt 中的日志数据 输出到两个文件中   
 * 	     带有"atguigu" 的日志数据  ==> d:/atguigu.log
 *    其他的日志数据 ==> d:/other.log
 *
 */		
public class LogFilterRecordWriter extends RecordWriter<Text, NullWritable> {
	
	private String  atguiguPath = "d:/atguigu.log";
	private String  otherPath  = "d:/other.log";
	
	private FSDataOutputStream atguiguOut  ;
	private FSDataOutputStream otherOut  ;
	
	
	public LogFilterRecordWriter(TaskAttemptContext context) {
		try {
			Configuration conf = context.getConfiguration();
			//获取文件系统对象
			FileSystem fs = FileSystem.get(conf);
			//创建输出流
		    atguiguOut  = fs.create(new Path(atguiguPath));
		    otherOut = fs.create(new Path(otherPath));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		//1. 获取一条日志数据
		String log = key.toString();
		//2.判断
		if(log.contains("atguigu")) {
			atguiguOut.writeBytes(log);
		}else {
			otherOut.writeBytes(log);
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
		IOUtils.closeStream(atguiguOut);
		IOUtils.closeStream(otherOut);
	}
	

}
