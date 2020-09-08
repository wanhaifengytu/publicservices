package com.sap.hadoop.client.spark;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import jodd.util.StringUtil;
import scala.Tuple2;

public class TestSparkSimple {

	public static void main(String[] args) {
		System.out.println("This is the start of Spark Simple program");
		String inputFile = "D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\pom.xml";
		runSimpleRDD(inputFile);

	}

	public static void runSimpleRDD(String inputFilePath) {
		/*
		 * This is the address of the Spark cluster. We will call the task from
		 * WordCountTest and we use a local standalone cluster. [*] means use all the
		 * cores available. See {@see
		 * http://spark.apache.org/docs/latest/submitting-applications.html#
		 * master-urls}.
		 */
		String master = "local[*]";

		/*
		 * Initialises a Spark context.
		 */
		SparkConf conf = new SparkConf().setAppName("TestSparkSimple").setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Performs a work count sequence of tasks and prints the output with a logger.
		 */
		JavaRDD<String> fileRDD = context.textFile(inputFilePath);
		JavaRDD<String> wordsRDD = fileRDD.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String text) throws Exception {
				System.out.println(" text " + text);
				List<String> wordsList = Arrays.asList(text.split(" "));
				return wordsList.iterator();
			}
		});
		System.out.println(" words RDD " + wordsRDD.count());
		JavaPairRDD<String, Long> wordsPairs = wordsRDD.mapToPair(word -> {
			return new Tuple2<>(StringUtil.capitalize(word), 1L);
		});

		JavaPairRDD<String, Long> wordCountRDD = wordsPairs.reduceByKey((a, b) -> {
			return a + b;
		});
		
		wordCountRDD.foreach(result -> {
			  System.out.println("result " + result._1 + " " + result._2);
		});

		// context.textFile(inputFilePath).flatMap(text -> Arrays.asList(text.split("
		// ")).iterator())
		// .mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey((a, b) -> a + b)
		// .foreach(result -> System.out.println(String.format("Word [%s] count [%d].",
		// result._1(), result._2)));
		System.out.println(" count " + wordsPairs.count());
	}

}
