package perf.spark.elastic.perf.spark.elastic;

import org.apache.spark.sql.api.java.UDF1;

public class GeneralizeURLUDFPerflog implements UDF1 {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		if (input == null) {
			return "";
		}
		
		String eachone = (String)input;
		if (eachone.indexOf("crb") > 0 || eachone.indexOf("user")>0 ||  eachone.indexOf("company")>0) {
			return CommonUtil.generalizeURL((String)input);
		} else {
			return input;
		}
		
	}
}
