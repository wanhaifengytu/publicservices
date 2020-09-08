package com.spark.analysis.logs;

import org.apache.spark.sql.api.java.UDF1;

public class GeneralizeURLUDF implements UDF1 {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		return CommonUtil.generalizeURLNew((String)input);
	}
}
