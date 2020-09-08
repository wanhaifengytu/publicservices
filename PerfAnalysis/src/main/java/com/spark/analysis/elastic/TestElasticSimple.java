package com.spark.analysis.elastic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import com.spark.analysis.logs.CommonUtil;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

//https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html
//https://github.com/elastic/elasticsearch-hadoop
public class TestElasticSimple {
	protected static Logger logger = Logger.getLogger(TestElasticSimple.class);

	public static void main(String[] args) throws Exception {

		SparkSession sparkSession = getSparkSession();

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		// if we use RDD
		//JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "monsoonperflogs");
		//System.out.println("count " + esRDD.count());
        //1. Test with Es Dataset
		testEsDatasetPerflogs(sparkSession);
		testEsDatasetServerlogs(sparkSession);
		testEsDatasetMetricBeatlogs(sparkSession);
		//2. Test streaming
		//testStreaming();
		
	}
	
	public static void testStreaming() throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");

		if (props.getProperty("hadoop.home.dir") != null) {
			System.setProperty("hadoop.home.dir", (String) props.getProperty("hadoop.home.dir"));
		}
		String master = (String) props.getProperty("master");
		SparkConf conf = new SparkConf().setMaster(master).setAppName("RealTimeSparkEs");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		//jssc.checkpoint("hdfs://10.116.28.128:9000/streaming_checkpoint");
		jssc.checkpoint((String) props.getProperty("checkPointPath"));

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list",(String) props.getProperty("kafkaBrokers"));

		System.out.println("Broker List " + kafkaParams.get("metadata.broker.list"));

		String kafkaTopics = "mnserverlogs,monsoonperflogs,nginxaccesslogs";
		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}
		for (String eachTopic : topics) {
			System.out.println("Each Topic " + eachTopic);
		}

		JavaPairInputDStream<String, String> logsStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		
		
		JavaPairDStream<String, String> mappedPairRDD = logsStream.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				String key = tuple._1;
				String value = tuple._2;
				
				System.out.println("This is the streaming");
				CommonUtil.writeLineToFile("received", key + value);
				return new Tuple2<String, String>(key,value);	
			}
		});
		
		mappedPairRDD.count();
		mappedPairRDD.print();
		
		
		jssc.start();
		jssc.awaitTermination();
	    jssc.close();
		
		
	}

	public static SparkSession getSparkSession() throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");

		if (props.getProperty("hadoop.home.dir") != null) {
			System.setProperty("hadoop.home.dir", (String) props.getProperty("hadoop.home.dir"));
		}
		String master = (String) props.getProperty("master");
		SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster(master)
				.set("es.index.auto.create", "true").set("es.nodes", (String) props.getProperty("esIPs"))
				.set("es.port", (String) props.getProperty("esPort")).set("es.nodes.wan.only", "true");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		return sparkSession;
	}

	public static void testEsDatasetPerflogs(SparkSession sparkSession) throws Exception {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs");
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");

		Dataset<Row> dataset = sparkSession.sql(
				"SELECT monsoonperflogs.CPU, monsoonperflogs.MEM FROM monsoonperflogs monsoonperflogs order by monsoonperflogs.CPU desc");
		System.out.println("got rows " + dataset.count());
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSample.csv", dataset);

		Dataset<Row> datasetNginx = sparkSession.read().format("org.elasticsearch.spark.sql").load("nginxaccesslogs");
		datasetNginx.createOrReplaceTempView("nginxaccesslogs");
		// System.out.println("nginx record count " + datasetNginx.count());
		Dataset<Row> nginxDs = sparkSession.sql(
				"SELECT nginxaccesslogs.message,nginxaccesslogs.log,  nginxaccesslogs.agent  FROM nginxaccesslogs nginxaccesslogs");
		nginxDs.show();

		// Write to Elastic
		// JavaEsSparkSQL.saveToEs(nginxDs, "newindexspark");
	}
	
	public static void testEsDatasetServerlogs(SparkSession sparkSession) throws Exception {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("mnserverlogs");
		datasetOrigin.createOrReplaceTempView("mnserverlogs");

		Dataset<Row> dataset = sparkSession.sql(
				"SELECT mnserverlogs.logdate, mnserverlogs.message, mnserverlogs.agent FROM mnserverlogs mnserverlogs");
		System.out.println("got rows " + dataset.count());
		CommonUtil.printTop5RowsOfDataset("mnserverlogsSample.csv", dataset);

		// Write to Elastic
		// JavaEsSparkSQL.saveToEs(nginxDs, "newindexspark");
	}
	
	//metricbeat-7.3.1
	public static void testEsDatasetMetricBeatlogs(SparkSession sparkSession) throws Exception {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("metricbeat-7.3.1");
		datasetOrigin.createOrReplaceTempView("metricbeatlogs");

		Dataset<Row> dataset = sparkSession.sql(
				"SELECT metricbeatlogs.host.name, metricbeatlogs.system.network.out.bytes FROM metricbeatlogs metricbeatlogs");
		System.out.println("got rows " + dataset.count());
		CommonUtil.printTop5RowsOfDataset("metricbeatlogsSample.csv", dataset);

		// Write to Elastic
		// JavaEsSparkSQL.saveToEs(nginxDs, "newindexspark");
	}

}
