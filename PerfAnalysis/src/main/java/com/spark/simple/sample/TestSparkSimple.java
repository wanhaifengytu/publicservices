package com.spark.simple.sample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.spark.analysis.logs.CommonUtil;

public class TestSparkSimple {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of spark program");
		Properties props =  CommonUtil.loadProperties("sparkParams.properties");
		
		if(props.getProperty("hadoop.home.dir")!=null) {
			System.setProperty("hadoop.home.dir", (String)props.getProperty("hadoop.home.dir"));
		}
		
		String master = (String)props.getProperty("master");
		
		SparkSession session = SparkSession.builder().appName("TestSparkSimple").master(master).getOrCreate();
		SparkContext sparkContext = session.sparkContext();
		 JavaSparkContext jsc = new JavaSparkContext(sparkContext);

		    int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		    int n = 100000 * slices;
		    List<Integer> l = new ArrayList<>(n);
		    for (int i = 0; i < n; i++) {
		      l.add(i);
		    }

		    JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);
		
        
		   System.out.println("Count  " + dataSet.count());

	}

}
