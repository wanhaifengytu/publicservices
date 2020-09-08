package com.sap.hadoop.client.spark.perfload.logs;

import org.apache.spark.sql.api.java.UDF1;

import com.sap.hadoop.client.spark.CommonUtil;

public class GeneralizeURLUDF implements UDF1 {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		return CommonUtil.generalizeURL((String)input);
	}
}
