package com.atguigu.mr.partitioner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, 
				Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		 long upSumFlow  = 0;
		 long downSumFlow = 0; 
		
		//计算 总上行  总下行  总流量
		for (FlowBean flowBean : values) {
			upSumFlow +=flowBean.getUpFlow();
			downSumFlow +=flowBean.getDownFLow();
		}
		
		//封装Bean
		FlowBean fb = new FlowBean(upSumFlow,downSumFlow);
		
		//写出
		context.write(key, fb);
	}
}
