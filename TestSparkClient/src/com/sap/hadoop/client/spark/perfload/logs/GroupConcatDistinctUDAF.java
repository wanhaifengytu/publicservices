package com.sap.hadoop.client.spark.perfload.logs;

import java.util.Arrays;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction {

	private static final long serialVersionUID = -2510776241322950505L;
	
	//Returned Type
	private DataType dataType = DataTypes.StringType;

	private boolean deterministic = true;
	
	// Input field and schema
	private StructType inputSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("requestURL", DataTypes.StringType, true)));  
  
	// Buffer field and schema
	private StructType bufferSchema = DataTypes.createStructType(Arrays.asList(
			DataTypes.createStructField("bufferRequestURL", DataTypes.StringType, true)));  
	
	@Override
	public StructType bufferSchema() {
		return bufferSchema;
	}

	@Override
	public DataType dataType() {
		return dataType;
	}

	@Override
	public boolean deterministic() {
		return deterministic;
	}

	@Override
	public Object evaluate(Row row) {
		return row.getString(0);  
	}

	@Override
	public void initialize(MutableAggregationBuffer buffer) {
		buffer.update(0, "");
	}

	@Override
	public StructType inputSchema() {
		
		return inputSchema;
	}

	@Override
	public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
		String bufferInfo1 = buffer1.getString(0);
		String bufferInfo2 = buffer2.getString(0);
		
		for(String info : bufferInfo2.split("_")) {
			if(!bufferInfo1.contains(info)) {
				if("".equals(bufferInfo1)) {
					bufferInfo1 += info;
				} else {
					bufferInfo1 += "_" + info;
				}
 			}
		}
		
		buffer1.update(0, bufferInfo1); 
	}

	@Override
	public void update(MutableAggregationBuffer buffer, Row input) {
		// Buffer: already concated string
		String bufferInfo = buffer.getString(0);
		// just input new string
		String info = input.getString(0);
		
		// Remove duplicate 
		// If not exist then 
		if(!bufferInfo.contains(info)) {
			if("".equals(bufferInfo)) {
				bufferInfo += info;
			} else {
				bufferInfo += "_" + info;
			}
			
			buffer.update(0, bufferInfo);  
		}
		
	}
}
