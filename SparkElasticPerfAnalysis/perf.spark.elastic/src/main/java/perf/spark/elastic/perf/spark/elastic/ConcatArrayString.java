package perf.spark.elastic.perf.spark.elastic;

import org.apache.spark.sql.api.java.UDF1;
import scala.collection.mutable.WrappedArray;

public class ConcatArrayString  implements UDF1  {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Object call(Object input) throws Exception {
		WrappedArray<String> strArray =  (WrappedArray<String>) input;
		
		scala.collection.immutable.List<String> list = strArray.toList();
		
		StringBuilder  strBuilder = new StringBuilder();
		for (int i=0; i<list.size(); i++) {
			strBuilder.append(list.apply(i)).append(" ");
		}
		
		return strBuilder.toString();
	}
	
}
