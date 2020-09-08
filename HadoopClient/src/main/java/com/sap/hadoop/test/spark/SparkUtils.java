package com.sap.hadoop.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Spark工具类
 * @author Administrator
 *
 */
public class SparkUtils {
	
	/**
	 * 根据当前是否本地测试的配置
	 * 决定，如何设置SparkConf的master
	 */
	public static void setMaster(SparkConf conf) {
		boolean local = true;
		if(local) {
			conf.setMaster("local");  
		}  
	}
	
	/**
	 * 获取SQLContext
	 * 如果spark.local设置为true，那么就创建SQLContext；否则，创建HiveContext
	 * @param sc
	 * @return
	 */
	public static SQLContext getSQLContext(SparkContext sc) {
		boolean local = true;
		if(local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
	
	
}
