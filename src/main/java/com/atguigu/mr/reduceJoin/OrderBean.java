package com.atguigu.mr.reduceJoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OrderBean implements Writable {
	
	private String orderId ;
	
	private String productId ;
	
	private String productName ;
	
	private Integer amount ;
	
	private String  title ;
	
	public OrderBean() {
	}
	

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getProductName() {
		return productName;
	}

	public void setProductName(String productName) {
		this.productName = productName;
	}

	public Integer getAmount() {
		return amount;
	}

	public void setAmount(Integer amount) {
		this.amount = amount;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(orderId);
		out.writeUTF(productId);
		out.writeUTF(productName);
		out.writeInt(amount);
		out.writeUTF(title);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderId = in.readUTF();
		productId = in.readUTF();
		productName = in.readUTF();
		amount = in.readInt();
		title = in.readUTF();
	}

	@Override
	public String toString() {
		return orderId +"\t" + productName +"\t"  + amount;
 	}
	
}
