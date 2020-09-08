package com.spark.analysis.logs;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.sql.api.java.UDF1;

public class TimeStampToStringUDF implements UDF1 {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		
		Date date=new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z").parse((String)input);
 
		//2019-10-30 09:44:57,755
		String formattedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z").format(date);

		return (Object)formattedDate;
	}
}
