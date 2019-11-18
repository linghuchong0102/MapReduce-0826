package com.atguigu.mr.myInputFormat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 自定义InputFormat, 需要继承FileInputFormat
 *
 */
public class SmallFileInputFormat extends FileInputFormat<Text, BytesWritable> {
	
	/**
	 * 设置指定的文件是否可切片. 
	 * 
	 * 现在的需求是将整个文件的内容作为value ，因此不能够对文件进行切片.
	 */
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	
	/**
	 * ·认规则
	 */

	
	/**
	 * 创建当前SamllFileInputFormat读取数据使用的RecordReader对象.
	 */
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		SmallFileRecordReader   recordReader  = new SmallFileRecordReader();
		
		recordReader.initialize(split, context);
		
		return recordReader;
	}

}
