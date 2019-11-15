package com.atguigu.mr.flowsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * 用于封装一个手机号对应的上行流量  下行流量  总流量
 */
public class FlowBean  implements Writable{
	
	private Long upFlow ;
	private Long downFLow ;
	private Long sumFlow ;
	
	public FlowBean() {
	}
	
	
	
	public FlowBean(Long upFlow, Long downFLow) {
		super();
		this.upFlow = upFlow;
		this.downFLow = downFLow;
		this.sumFlow = upFlow + downFLow;
	}



	public Long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(Long upFlow) {
		this.upFlow = upFlow;
	}
	public Long getDownFLow() {
		return downFLow;
	}
	public void setDownFLow(Long downFLow) {
		this.downFLow = downFLow;
	}
	public Long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(Long sumFlow) {
		this.sumFlow = sumFlow;
	}
	
	/**
	 * 序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFLow);
		out.writeLong(sumFlow);
	}
	
	/**
	 * 反序列化方法
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		downFLow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public String toString() {
		return  upFlow+"\t"+downFLow+"\t"+sumFlow;
	}
	
}
