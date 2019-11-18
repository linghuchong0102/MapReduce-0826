package com.atguigu.mr.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区器 需要继承Partitioner ,重写getPartition方法
 * @author Administrator
 *
 */
public class PhoneNumberPartitioner extends Partitioner<Text, FlowBean> {
	/**
	 * 136  分区0
	 * 137  分区1 
	 * 138  分区2
	 * 139  分区3
	 * 其他   分区4
	 */
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		int partition ;
		String keyStr = key.toString();
		String phonePre =  keyStr.substring(0, 3);
		if("136".equals(phonePre)) {
			partition = 0 ;
		}else if ("137".equals(phonePre)) {
			partition = 1 ;
		}else if("138".equals(phonePre)) {
			partition = 2 ;
		}else if ("139".equals(phonePre)) {
			partition = 3 ;
		}else {
			partition = 4 ;
		}
		
		return partition;
	}
}










