package com.atguigu.mr.reduceJoin;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinReducer  extends Reducer<Text, OrderBean, OrderBean, NullWritable>{
	
	//List<OrderBean> orders = new ArrayList<>();
	
	@Override
	protected void reduce(Text key, Iterable<OrderBean> values,
			Reducer<Text, OrderBean, OrderBean, NullWritable>.Context context) throws IOException, InterruptedException {
		
		List<OrderBean> orders = new ArrayList<>();
		
		OrderBean pdBean = new OrderBean();
		
		//迭代获取每个OrderBean对象
		for (OrderBean orderBean : values) {
			if("order".equals(orderBean.getTitle())) {
				try {
					OrderBean newOrder  = new OrderBean();
					BeanUtils.copyProperties(newOrder, orderBean);
					orders.add(newOrder);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}else {
				try {
					BeanUtils.copyProperties(pdBean, orderBean);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		
		//从pdBean中取出productName,赋值给orders中的每个OrderBean上.
		
		for (OrderBean orderBean : orders) {
			orderBean.setProductName(pdBean.getProductName());
			//写出
			context.write(orderBean, NullWritable.get());
		}
	
	}
}
