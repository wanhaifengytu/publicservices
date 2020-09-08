package perf.spark.elastic.perf.spark.elastic;

import org.apache.spark.sql.api.java.UDF4;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;

public class CombineVectorColumnsUDF  implements UDF4  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1663254232324L;

	//ucpu, mem, rqt, result
	@Override
	public Object call(Object t1, Object t2, Object t3, Object t4) throws Exception {
		double[] numbers  = new double[3];
		
		//StringBuilder strBuilder = new StringBuilder();
		
		//strBuilder.append(Float.parseFloat(t1.toString())).append(",");
		//strBuilder.append(Float.parseFloat(t2.toString())).append(",");
		//strBuilder.append(Float.parseFloat(t3.toString())).append(",");
		numbers[0] = Double.parseDouble(t1.toString());
		numbers[1] = Double.parseDouble(t2.toString()); 
		numbers[2] = Double.parseDouble(t3.toString());
		 
		Vector vec = (DenseVector)t4;
	
		return ArrayUtils.addAll(numbers, vec.toArray());
		//return CommonUtil.combineTwoVectors(numbers, (DenseVector)t4);
	}

	

}
