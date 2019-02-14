package com.sap.hadoop.test.mapreduce.mobileflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean  implements Writable {

	private long upFlow;
	private long dFlow;
	
	private long sumFlow;
	
	public FlowBean() {
		
	}
	public FlowBean(long upFlow, long dFlow) {
		super();
		this.upFlow = upFlow;
		this.dFlow = dFlow;
		this.sumFlow = this.upFlow + this.dFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getdFlow() {
		return dFlow;
	}

	public void setdFlow(long dFlow) {
		this.dFlow = dFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	
	@Override
	public String toString() {
		return  this.upFlow + "\t" + this.dFlow + "\t" + this.sumFlow;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		upFlow = input.readLong();
		dFlow = input.readLong();
		sumFlow = input.readLong();
		
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeLong(upFlow);
		output.writeLong(dFlow);
		output.writeLong(sumFlow);
	}
}
