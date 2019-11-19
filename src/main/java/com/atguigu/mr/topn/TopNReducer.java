package com.atguigu.mr.topn;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<FlowBean, Text, Text, FlowBean> {
	
	TreeMap<FlowBean,Text> maps = new TreeMap<>();
 
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Reducer<FlowBean, Text, Text, FlowBean>.Context arg2)
			throws IOException, InterruptedException {
		//迭代一组数据
		for (Text text : values) {
			try {
				//封装key
				FlowBean currentKey = new FlowBean();
				BeanUtils.copyProperties(currentKey, key);
				
				//封装value
				Text  value = new Text();
				value.set(text.toString());
				
				//存储到map中
				maps.put(currentKey, value);
				
				//判断TreeMap中的长度是否大于10 
				if(maps.size()>10) {
					//移除最后一个
					maps.remove(maps.lastKey());
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
	}
	
	
	@Override
	protected void cleanup(Reducer<FlowBean, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		
		//迭代TreeMap
		Set<Entry<FlowBean, Text>> entrySet = maps.entrySet();
		for (Entry<FlowBean, Text> entry : entrySet) {
			//获取key
			FlowBean key = entry.getKey();
			//获取value
			Text value = entry.getValue();
			
			//写出
			context.write(value, key);
		}
	}
}
