package com.sap.elastic.client.test;

import java.util.Date;
import java.util.List;

import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.spark_project.guava.collect.ImmutableList;

public class SimpleElasticClient {
	//public static String esIPs = "10.116.29.96,10.116.28.152,10.116.31.203";
	public static String esIPs = "10.116.29.96";
	public static String esPort = "9200";

	public static void main(String[] args) throws Exception {
		System.out.println(" this is the start of the program");
		// with bin/winutils.exe in this folder.
		System.setProperty("hadoop.home.dir", "C:\\tools");

		// createIndexDemo(getClient(), "patricNewIndex");
		// javaESSparkTest();
		//readESBySql();
		readPerflogFromESFromDate(new Date());
	}

	public static void javaESSparkTest() {
		SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local[*]")
				.set("es.index.auto.create", "true").set("es.nodes", esIPs).set("es.port", esPort)
				.set("es.nodes.wan.only", "true");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		String json1 = "{\"reason\" : \"business\",\"airport\" : \"SFO\"}";
		String json2 = "{\"participants\" : 5,\"airport\" : \"OTP\"}";

		// JavaContextSpark jsc = ...
		// JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2));
		// JavaEsSpark.saveJsonToEs(stringRDD, "spark/json-trips");
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());// adapter
		JavaRDD<String> stringRDD = jsc.parallelize(ImmutableList.of(json1, json2));
		JavaEsSpark.saveJsonToEs(stringRDD, "testpatricindex/mydoctype");

	}

	public static void readESBySql() {
		SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local[*]")
				.set("es.index.auto.create", "true").set("es.nodes", esIPs).set("es.port", esPort)
				.set("es.nodes.wan.only", "true");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());// adapter
		
		Dataset<Row> ds = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonvmloadlogs/doc");
		ds.registerTempTable("mvmloadlogs");
		SQLContext sqlContext = new SQLContext(jsc);
		Dataset<Row> dataset = sqlContext.sql("SELECT mvmlogs.cs, mvmlogs.us FROM mvmloadlogs mvmlogs");
		List<Row> top5Lines = dataset.takeAsList(5);
		for (Row eachLine : top5Lines) {
			System.out.println("each Line " + eachLine.getString(0) + " " + eachLine.getString(1));
		}
	}
	
	public static void readPerflogFromESFromDate(Date dateStart) {
		SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local[*]")
				.set("es.index.auto.create", "true").set("es.nodes", esIPs).set("es.port", esPort)
				.set("es.nodes.wan.only", "true");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		
		Dataset<Row> ds = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs/doc");
		ds.registerTempTable("mnperflogs");
		
		SQLContext sqlContext = new SQLContext(jsc);
		Dataset<Row> dataset = sqlContext.sql("SELECT mnperflogs.URL,mnperflogs.SQLT, mnperflogs.RQT,mnperflogs.logdate FROM mnperflogs mnperflogs where mnperflogs.logdate>\"2019-07-19\" order by mnperflogs.logdate desc");
		System.out.println("got rows " + dataset.count());
		List<Row> top5Lines = dataset.takeAsList(15);
		for (Row eachLine : top5Lines) {
			System.out.println("each Line " + eachLine.getString(0) + " " + eachLine.getString(1) + " " + eachLine.getString(2) + " " + eachLine.getString(3));
		}
		
	}

	/**
	 * Highlevel ESClient
	 * 
	 * @return
	 */
	public static RestHighLevelClient getClient() {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("10.116.29.96", 9200, "http"),
						new HttpHost("10.116.28.152", 9200, "http"), new HttpHost("10.116.31.203", 9200, "http")));
		return client;
	}

	public static void createIndexDemo(RestHighLevelClient client, String indexName) throws Exception {

		CreateIndexRequest request = new CreateIndexRequest(indexName);
		//
		request.alias(new Alias("mmm"));

		//
		CreateIndexResponse createIndexResponse = client.indices().create(request);

		// 6
		boolean acknowledged = createIndexResponse.isAcknowledged();
		boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
		System.out.println("acknowledged = " + acknowledged);
		System.out.println("shardsAcknowledged = " + shardsAcknowledged);

	}
}
