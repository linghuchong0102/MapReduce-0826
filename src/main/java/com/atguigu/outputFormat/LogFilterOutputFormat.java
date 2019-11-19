package com.atguigu.outputFormat;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义OutputFormat , 需要继承FileOutputFormat
 */
public class LogFilterOutputFormat extends FileOutputFormat<Text, NullWritable> {

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		LogFilterRecordWriter recordWriter = new LogFilterRecordWriter(job);
		
		return recordWriter;
	}
	
	
}
