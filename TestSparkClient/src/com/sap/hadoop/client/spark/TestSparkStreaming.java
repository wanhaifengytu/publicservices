package com.sap.hadoop.client.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class TestSparkStreaming {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of spark streaming");
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");

		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		jssc.checkpoint("hdfs://10.116.28.128:9000/streaming_checkpoint");

		Map<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "10.116.28.55:9092");

		System.out.println("Broker List " + kafkaParams.get("metadata.broker.list"));

		String kafkaTopics = "AdRealTimeLogNew";
		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Set<String> topics = new HashSet<String>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topics.add(kafkaTopic);
		}
		for (String eachTopic : topics) {
			System.out.println("Each Topic " + eachTopic);
		}

		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(jssc, String.class,
				String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

/*		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream);

		filteredAdRealTimeLogDStream.foreachRDD(new Function<JavaPairRDD<String,String>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, String> rdd) throws Exception {
				 System.out.println("rdd " + rdd.count());
				return null;
			}			
	}); */
		
		JavaPairDStream<String, String> mappedPairRDD = adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
			@Override
			public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
				String key = tuple._1;
				String value = tuple._2;
				
				System.out.println("This is the streaming");
				
				String[] logSplited = value.split(" ");
				
				
				return new Tuple2<String, String>(logSplited[0]+ "_"+ logSplited[1] + "_" + logSplited[2],logSplited[3] + "_" + logSplited[4]);	
			}
		});
 
 
		
		/*mappedPairRDD.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
			@Override
			public Void call(JavaPairRDD<String, String> tuple) throws Exception {
				System.out.println(" t " + tuple.toString());
				return null;
			}
		}); */
		
		
		jssc.start();
		jssc.awaitTermination();
		//Thread.sleep(50000);
	    jssc.close();
	}

	private static JavaPairDStream<String, String> filterByBlacklist(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {

		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(

				new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {

					private static final long serialVersionUID = 1L;

					@SuppressWarnings("resource")
					@Override
					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {

						// 将原始数据rdd映射成<userid, tuple2<string, string>>
						JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd
								.mapToPair(new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {

									private static final long serialVersionUID = 1L;

									@Override
									public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple)
											throws Exception {
										String log = tuple._2;
										String[] logSplited = log.split(" ");
										long userid = Long.valueOf(logSplited[3]);
										System.out.println("Received " + log);
										return new Tuple2<Long, Tuple2<String, String>>(userid, tuple);
									}

								});

						// 将原始日志数据rdd，与黑名单rdd，进行左外连接
						// 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
						// 用inner join，内连接，会导致数据丢失

						JavaPairRDD<String, String> resultRDD = mappedRDD
								.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, String>>, String, String>() {
									@Override
									public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, String>> tuple)
											throws Exception {
										return tuple._2;
									}
								});
						// mappedRDD.mapToPair(new PairFunction<Tuple2>)

						return resultRDD;
					}

				});

		return filteredAdRealTimeLogDStream;
	}

}
