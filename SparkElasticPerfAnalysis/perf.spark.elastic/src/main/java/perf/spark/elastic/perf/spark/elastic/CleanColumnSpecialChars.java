package perf.spark.elastic.perf.spark.elastic;

import org.apache.spark.sql.api.java.UDF1;

public class CleanColumnSpecialChars implements UDF1  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		return CommonUtil.cleanUnusedChars((String)input);
	}
}
