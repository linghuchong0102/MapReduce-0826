package com.atguigu.mr.comparable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhoneNumberPartitioner  extends Partitioner<FlowBean, Text>{
	
	@Override
	public int getPartition(FlowBean key, Text value, int numPartitions) {
		int result ;
		String phoneStr = value.toString();
		String phonePre = phoneStr.substring(0, 3);
		
		if("136".equals(phonePre)) {
			result = 0 ;
		}else if ("137".equals(phonePre)) {
			result = 1 ; 
		}else if ("138".equals(phonePre)) {
			result = 2 ; 
		}else if ("139".equals(phonePre)) {
			result = 3 ;
		}else {
			result = 4 ;
		}
		
		return result ;
	}
}
