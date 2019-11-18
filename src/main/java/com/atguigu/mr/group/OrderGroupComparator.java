package com.atguigu.mr.group;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组比较器 需要继承 WritableComparator ,
 * 		重写compare(WritableComparable a, WritableComparable b)方法.
 * 		还需要通过构造器将Key的类型与分组比较器绑定
 * 
 * 1. WritableComparator中的compare方法默认会使用Key对象的compareTo方法进行比较. 
 * 
 */
public class OrderGroupComparator extends WritableComparator {
	
	public OrderGroupComparator() {
		super(OrderBean.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean aBean = (OrderBean)a ;
		OrderBean bBean = (OrderBean)b ;
		int result  ;
		if(aBean.getOrderId() > bBean.getOrderId()) {
			result = 1 ;
		}else  if(aBean.getOrderId() < bBean.getOrderId()){
			result = -1 ;
		}else {
			result =0 ;
		}
		return result ;
	}
	
}	
