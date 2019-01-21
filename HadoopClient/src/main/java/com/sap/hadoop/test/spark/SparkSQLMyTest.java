package com.sap.hadoop.test.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class SparkSQLMyTest {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of main program");

		// Create SparkConf
		SparkConf conf = new SparkConf().setAppName("SparkSQLMyTest");
		SparkUtils.setMaster(conf);

		// Create SparkContext
		JavaSparkContext sparkContext = new JavaSparkContext(conf);
		SQLContext sqlContext = SparkUtils.getSQLContext(sparkContext.sc());

		String inputFilePath = "C:\\tools\\Eclipse\\ws\\HadoopClient\\perflogsimple.txt";
		JavaRDD<String> fileContentsRDD = sparkContext.textFile(inputFilePath);
		//System.out.println("line count" + fileContentsRDD.count());

		JavaRDD<Row> fileRowsRDD = fileContentsRDD.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row call(String line) throws Exception {
				String dateTime = line.substring(0, 23);
				//System.out.println("Date time " + dateTime);
				String remainingLine = line.substring(38).trim();
				
				//Grab "STK=" if exists.
				String stk = null;
				if (remainingLine.indexOf("STK=") >=0) {
					stk = remainingLine.substring(remainingLine.indexOf("STK="));
				}
				//System.out.println("stk " + stk);
				
				return RowFactory.create(dateTime, stk);
			}
		});

		//For each row, do a kind of action
		fileRowsRDD.foreach(row -> {
			System.out.println("row date " +  row.getString(0));
		});
		/*
		 * JavaRDD<Row> fileRowsRDD = fileContentsRDD.map(line -> {
		 * System.out.println("Line " + line);
		 * 
		 * return RowFactory.create(cityid, cityName, area, productid); //
		 * return new Tuple2<>(line, 1); }) ;
		 */

		sparkContext.close();
	}
}
