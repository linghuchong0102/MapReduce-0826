package com.atguigu.mr.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class OrderBean implements WritableComparable<OrderBean> {
	
	private Integer orderId;
	
	private Double price;
	
	public OrderBean() {
	}
	public Integer getOrderId() {
		return orderId;
	}

	public void setOrderId(Integer orderId) {
		this.orderId = orderId;
	}

	public Double getPrice() {
		return price;
	}

	public void setPrice(Double price) {
		this.price = price;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeInt(orderId);
		out.writeDouble(price);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		orderId = in.readInt();
		price = in.readDouble();
	}
	
	/**
	 * 比较规则:按照价格倒叙
	 * 
	 * 考虑到分组: 需要先按照id排序，再按照价格排序。
	 */
	@Override
	public int compareTo(OrderBean o) {
		int result  ;
		
		if(this.getOrderId() > o.getOrderId()) {
			result = 1 ;
		}else if (this.getOrderId() < o.getOrderId()) {
			result = -1 ;
		}else {
			if(this.getPrice() > o.getPrice()) {
				result = -1 ;
			}else if(this.getPrice() < o.getPrice()) {
				result = 1 ; 
			}else {
				result = 0 ;
			}
		}
		return result;
	}
	@Override
	public String toString() {
		return  orderId +"\t" + price;
	} 
	
	
}
