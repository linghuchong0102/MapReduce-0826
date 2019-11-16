package com.atguigu.mr.myInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 自定义RecordReader , 需要继承RecordReader类。
 * 
 * 需求: 将一个小文件中的内容全部读取，作为value, 小文件的路径+名字作为key
 *
 */
public class SmallFileRecordReader  extends RecordReader<Text, BytesWritable>{
	
	private FileSplit currentFileSplit; //当前的切片对象
	
	private Configuration conf ; // 配置对象
	
	private Text currentKey = new Text() ; //当前的key
	
	private BytesWritable currentValue = new BytesWritable() ; 
	
	private boolean flag = true ;
	
	/**
	 * 初始化
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		//获取到当前的切片对象
		currentFileSplit  = (FileSplit)split;
		
		//获取Configuration配置对象
		conf = context.getConfiguration();
	}
	
	/**
	 *  读取下一个key 和 value
	 *  
	 *  明确当前处理的文件是哪一个， 并且要将文件中的内容全部读取
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		
		if(flag) {
			//获取文件的路径+名字
			String  filePath = currentFileSplit.getPath().toString();
			//封装key
			currentKey.set(filePath);
			
			//封装value
			//获取文件系统
			FileSystem fs = FileSystem.get(conf);
			//获取到输入流
			FSDataInputStream in = fs.open(currentFileSplit.getPath());
			//创建存储文件内容的字节数据
			byte [] dataArray = new byte[(int)currentFileSplit.getLength()] ;
			IOUtils.readFully(in, dataArray,0, dataArray.length);
			
			currentValue.set(dataArray, 0, dataArray.length);
			
			flag = false ; 
			
			return true ;
			
		}
		return false;
	}
	
	/**
	 * 获取当前key
	 */
	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentKey;
	}
	/**
	 * 获取当前Value
	 */
	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return currentValue;
	}
	
	/**
	 * 获取Map的进度
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * 关闭
	 */
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
