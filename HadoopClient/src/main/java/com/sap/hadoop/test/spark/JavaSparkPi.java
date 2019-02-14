package com.sap.hadoop.test.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Computes an approximation to pi Usage: JavaSparkPi [partitions]
 */
public final class JavaSparkPi {

	private static final long serialVersionUID = 123456789L;

	public static void main(String[] args) throws Exception {
		
		//SparkConf conf = new SparkConf().setMaster("spark://10.1.1.100:7077").setAppName("Word Count");
		SparkConf conf = new SparkConf().setAppName("JavaSparkPi");
		// Create a Java version of the Spark Context
		JavaSparkContext sparkContext = new JavaSparkContext(conf);

		int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 12;
		int n = 999999 * slices;
		List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		JavaRDD<Integer> dataSet = sparkContext.parallelize(l, slices);

		int count = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y <= 1) ? 1 : 0;
		}).reduce((integer, integer2) -> integer + integer2);

		System.out.println("Pi is roughly " + 4.0 * count / n);

		// spark.stop();
	}
}