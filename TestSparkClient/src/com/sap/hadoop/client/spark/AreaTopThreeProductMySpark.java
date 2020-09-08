package com.sap.hadoop.client.spark;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.fasterxml.jackson.databind.JsonDeserializer;

import scala.Tuple2;

public class AreaTopThreeProductMySpark {

	public static void main(String[] args) throws Exception {

		Properties configProperties = loadProperties("config.properties");

		// System.setProperty("hadoop.home.dir", "E:\\patric\\winutil");

		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		// 10.169.68.52
		// jssc.checkpoint("C://streaming_checkpoint2");
		jssc.checkpoint((String) configProperties.get("hdfs.checkpoint"));

		String kafkaServer = (String) configProperties.get("kafka.server");

		// Map<String, String> kafkaParams = new HashMap<String, String>();
		// kafkaParams.put("metadata.broker.list", kafkaServer);

		String kafkaTopics = (String) configProperties.get("kafka.topics");
		String[] kafkaTopicsSplited = kafkaTopics.split(",");

		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		for (String kafkaTopic : kafkaTopicsSplited) {
			topicMap.put(kafkaTopic, 3);
		}

		Map<String, Object> kafkaParams = new HashMap();
		kafkaParams.put("bootstrap.servers", kafkaServer);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", JsonDeserializer.class);
		kafkaParams.put("group.id", "rating_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		Collection<String> topics = Arrays.asList(kafkaTopicsSplited);

		String zkQuorum = (String) configProperties.get("zk.quorum");
		String groupdId = "consumer.groupId";
		JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createStream(jssc, zkQuorum, groupdId,
				topicMap);
		// JavaPairInputDStream<String, String> adRealTimeLogDStream =
		// KafkaUtils.createDirectStream(jssc, String.class,
		// String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

		JavaPairDStream<String, String> messagePair = echoReceivedMessage(adRealTimeLogDStream);

		JavaPairDStream<String, String> filteredMessagePair = filterByBlackList(adRealTimeLogDStream);

		generateDynamicBlacklist(filteredMessagePair);

		// Aggregate by (yyyyMMdd_province_city_adid,clickCount)
		JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(filteredMessagePair);

		// More detailed analysis
		calculateProvinceTop3Ad(adRealTimeStatDStream);

		// Calculate last one hour, every ad click tendency
		calculateAdClickCountByWindow(adRealTimeLogDStream);
		/*
		 * JavaDStream<Long> countAfterFilter = filteredMessagePair.count();
		 * countAfterFilter.print();
		 * 
		 * JavaPairDStream<Tuple2<String, String>, Long> countByValueRes =
		 * messagePair.countByValue(); countByValueRes.print();
		 */

		jssc.start();
		jssc.awaitTermination();
	}

	private static JavaPairDStream<String, String> echoReceivedMessage(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		JavaPairDStream<String, String> echoAdRealTimeLogDStream = adRealTimeLogDStream
				.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
					@Override
					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
						writeLineToFile("echoReceivedMessage.log", "1.Write a start " + rdd.count());
						JavaPairRDD<String, String> retRdd = rdd.mapToPair(row -> {
							writeLineToFile("echoReceivedMessage.log", "received message: " + row);
							return new Tuple2<String, String>(row._2, row._1);
						});
						retRdd.saveAsTextFile("echoReceivedMessage.txt");
						return retRdd;
					}
				});
		echoAdRealTimeLogDStream.print();
		return echoAdRealTimeLogDStream;
	}

	private static JavaPairDStream<String, String> filterByBlackList(
			JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		List<Tuple2<Long, Boolean>> blackListTuples = new ArrayList<Tuple2<Long, Boolean>>();
		blackListTuples.add(new Tuple2<Long, Boolean>(new Long(1052), true));
		blackListTuples.add(new Tuple2<Long, Boolean>(new Long(1012), true));
		blackListTuples.add(new Tuple2<Long, Boolean>(new Long(1011), true));
		blackListTuples.add(new Tuple2<Long, Boolean>(new Long(1030), true));
		blackListTuples.add(new Tuple2<Long, Boolean>(new Long(1038), true));

		JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream
				.transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
					@Override
					public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
						JavaSparkContext sc = new JavaSparkContext(rdd.context());
						JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(blackListTuples);

						writeLineToFile("filterByBlackList.log", "1.Started transformToPair Rdd" + rdd.count());

						// Map the original RDD to (userId, OriginTuple(***))
						JavaPairRDD<Long, Tuple2<String, String>> userIDPairTuples = rdd.mapToPair(row -> {
							String info2 = row._2;
							String[] infoList = info2.split(" ");
							Long userId = 0L;
							if (infoList.length == 5) {
								userId = new Long(infoList[4]);
							}
							writeLineToFile("filterByBlackList.log", "2.Map to Pair " + userId + " " + row);
							return new Tuple2<Long, Tuple2<String, String>>(userId, row);
						});

						// Join the RDD with blacklist RDD
						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, org.apache.spark.api.java.Optional<Boolean>>> joinedRDD = userIDPairTuples
								.leftOuterJoin(blacklistRDD);
						joinedRDD.saveAsTextFile("filterByBlackList_joinedRDD.txt");
						writeLineToFile("filterByBlackList.log", "3.joinedRDD " + joinedRDD.toString());

						JavaPairRDD<Long, Tuple2<Tuple2<String, String>, org.apache.spark.api.java.Optional<Boolean>>> filterRDD = joinedRDD
								.filter(row -> {
									org.apache.spark.api.java.Optional<Boolean> optional = row._2._2;

									// If this one exist . then might have been joined a blacklist name.
									if (optional.isPresent() && optional.get()) {
										return false;
									}
									return true;
								});

						JavaPairRDD<String, String> resultRDD = filterRDD.mapToPair(row -> {
							writeLineToFile("filterByBlackList.log", "4.filteredRDD mapToPair  " + row);
							return row._2._1;
						});

						return resultRDD;
					}

				});
		return filteredAdRealTimeLogDStream;
	}

	/**
	 * 
	 * @param filteredAdRealTimeLogDStream
	 */
	private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
		// AdRealTimeLog3 1556448959434 Hebei Shijiazhuang 22 1058 ,384
		JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) {

						writeLineToFile("generateDynamicBlacklist.log", "1.start processing  " + tuple);

						String info = tuple._2;
						String[] listStr = info.split(" ");
						System.out.println("ListStr " + listStr.length + " " + info);
						if (listStr.length == 5) { // + "_" + listStr[4]
							String key = getDateHourString(listStr[0]) + "_" + listStr[3];

							return new Tuple2<String, Long>(key, 1L);
						} else {
							return null;
						}
					}
				});
		dailyUserAdClickDStream.print();
		/*
		 * dailyUserAdClickDStream.foreachRDD(new Function<JavaPairRDD<String, Long>,
		 * Void>() { private static final long serialVersionUID = 1L;
		 * 
		 * @Override public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
		 * rdd.foreach(eachOne -> { if (eachOne != null) {
		 * System.out.println("EachOne got " + eachOne._1); } }); return null; } });
		 */

		// Reduce by Key: TimeHour_userId_adId , acumulate one user click one Ad within
		// one hour.
		JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream
				.reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						writeLineToFile("generateDynamicBlacklist.log", "2.start processing  " + v1 + " " + v2);
						return v1 + v2;
					}
				});
		dailyUserAdClickCountDStream.print();
		/*
		 * dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,
		 * Long>, Void>() { private static final long serialVersionUID = 1L;
		 * 
		 * @Override public Void call(JavaPairRDD<String, Long> rdd) throws Exception {
		 * rdd.foreach(eachOne -> { if (eachOne != null) { System.out
		 * .println("EachOne got of reduced User click Ad Count " + eachOne._1 + " " +
		 * eachOne._2); } }); return null; } });
		 */

		// ToDO: Filter according to click count > 2

	}

	/**
	 * 
	 * @param filteredAdRealTimeLogDStream
	 * @return
	 */
	private static JavaPairDStream<String, Long> calculateRealTimeStat(
			JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
		// 1556448959434 Hebei Shijiazhuang 22 1058 ,384
		// map to <date_province_city_adid,1> format
		writeLineToFile("calculateRealTimeStat.log", "0.started  " + filteredAdRealTimeLogDStream.count());
		JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) {

						String info = tuple._2;
						String[] listStr = info.split(" ");
						writeLineToFile("calculateRealTimeStat.log", "1.mapToPair  " + tuple);

						if (listStr.length == 5) { // + "_" + listStr[4]
							String key = getDateHourString(listStr[0]) + "_" + listStr[1] + "_" + listStr[2] + "_"
									+ listStr[3];
							return new Tuple2<String, Long>(key, 1L);
						} else {
							return null;
						}
					}
				});
		mappedDStream.print();
		/**
		 * updateStateByKey **** Function specified in updateStateByKey is applied to
		 * all the existing keys in every batch, regardless of whether they have a new
		 * data or not, if the update function return null, the key value will be
		 * eliminated. The function updates the previous state of all the key with the
		 * new value if present http://amithora.com/spark-update-by-key-explained/
		 */
		// https://www.datasciencebytes.com/bytes/2016/04/30/creating-a-spark-streaming-application-in-java/
		// https://www.jianshu.com/p/03f35edf0462
		/// List<Long> Optional<Map<String,Long>>
		JavaPairDStream<String, Long> aggregatedDStream = mappedDStream
				.updateStateByKey(new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Optional<Long> call(List<Long> values, Optional<Long> states) throws Exception {
						writeLineToFile("calculateRealTimeStat.log",
								"2.updateStateByKey  new:" + values + " old:" + states);
						Long newValue = 0L;
						if (states.isPresent()) {
							newValue = states.get();
						}

						for (Long value : values) {
							newValue += value;
						}
						return Optional.of(newValue);
					}
				});
		aggregatedDStream.print();
		return aggregatedDStream; // aggregatedDStream;

	}

	private static void calculateProvinceTop3Ad(JavaPairDStream<String, Long> adRealTimeStatDStream) {

		writeLineToFile("calculateProvinceTop3Ad.log",
				"1. Begin calculate province top 3 ad " + adRealTimeStatDStream.count());
		JavaDStream<Row> rowsDStream = adRealTimeStatDStream
				.transform(new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {
						// <date_province_city_adid,1>
						// rdd.foreach(eachRdd -> {
						// writeLineToFile("calculateProvinceTop3Ad.log", "2. received Rdd " + eachRdd);
						// });

						// (1194011_Hebei_Shijiazhuang_22,4)
						JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(eachOne -> {
							writeLineToFile("calculateProvinceTop3Ad.log", "2.map to pair " + eachOne);
							String[] keySplited = eachOne._1.split("_");
							String date = keySplited[0];
							String province = keySplited[1];
							long adid = Long.valueOf(keySplited[3]);
							long clickCount = Long.valueOf(eachOne._2);

							String key = date + "_" + province + "_" + adid;
							writeLineToFile("calculateProvinceTop3Ad.log",
									"2.map to pair old one:" + eachOne + " newOne:" + key + " " + clickCount);
							return new Tuple2<String, Long>(key, clickCount);
						});

						// JavaRDD<String> retRDD = mappedRDD.map(eachRow -> {
						// return eachRow._1;
						// });

						JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD
								.reduceByKey((value1, value2) -> {
									return value1 + value2;
								});

						// Register a temp table from <date_province_city_adid,count>
						JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(eachRec -> {
							String[] keySplited = eachRec._1.split("_");
							String datekey = keySplited[0];
							String province = keySplited[1];
							long adid = Long.valueOf(keySplited[2]);
							long clickCount = eachRec._2;

							return RowFactory.create(datekey, province, adid, clickCount);
						});

						StructType schema = DataTypes.createStructType(
								Arrays.asList(DataTypes.createStructField("date", DataTypes.StringType, true),
										DataTypes.createStructField("province", DataTypes.StringType, true),
										DataTypes.createStructField("ad_id", DataTypes.LongType, true),
										DataTypes.createStructField("click_count", DataTypes.LongType, true)));
						/*
						 * if(local) { return new SQLContext(sc); } else { return new HiveContext(sc); }
						 */

						SQLContext sqlContext = new SQLContext(rdd.context());
						Dataset dailyAdClickCountByProvinceDF = sqlContext.createDataFrame(rowsRDD, schema);
						// register dailyAdClickCountByProvinceDF to a temp table
						dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");

						String sql = "SELECT date, province, ad_id, click_count, "
								+ "row_number() OVER  (PARTITION BY province ORDER BY click_count DESC) rank"
								+ " FROM tmp_daily_ad_click_count_by_prov ";
						/*
						 * String sql = "SELECT " + "date," + "province," + "ad_id," + "click_count " +
						 * "FROM ( " + "SELECT " + "date," + "province," + "ad_id," + "click_count," +
						 * "row_number() OVER(PARTITION BY province ORDER BY click_count DESC) rank " +
						 * "FROM tmp_daily_ad_click_count_by_prov " + ") t " + "WHERE rank>=3";
						 */

						Dataset provinceTop3AdDF = sqlContext.sql(sql);
						provinceTop3AdDF.foreach(eachOne -> {
							writeLineToFile("calculateProvinceTop3Ad.log", "3. execute query got provinceTop3AdDF:" + eachOne);
						});
						
						return provinceTop3AdDF.javaRDD();
					}
				});

		rowsDStream.print();

	}

	/**
	 * 
	 * @param adRealTimeLogDStream
	 */
	private static void calculateAdClickCountByWindow(JavaPairInputDStream<String, String> adRealTimeLogDStream) {
		writeLineToFile("calculateAdClickCountByWindow.log", "1. start calculateAdClickCountByWindow:" + adRealTimeLogDStream.count());
		JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
						
						
						String info = tuple._2;
						String[] listStr = info.split(" ");
						System.out.println("ListStr " + listStr.length + " " + info);
						if (listStr.length == 5) { // + "_" + listStr[4]
							// day_adId, count
							String key = getDateHourString(listStr[0]) + "_" + listStr[4];
							
							writeLineToFile("calculateAdClickCountByWindow.log", "2. map to pair:" + key);
							return new Tuple2<String, Long>(key, 1L);
						} else {
							return null;
						}

					}
				});
		pairDStream.print();
		
		//
		// windowDuration,slideDuration
		JavaPairDStream<String,Long> reducedPair = pairDStream.reduceByKeyAndWindow((value1, value2) -> {
			writeLineToFile("calculateAdClickCountByWindow.log", "3.reduceByKeyAndWindow:" + value1 + " " + value2);
			return value1 + value2;
		}, Durations.minutes(60), Durations.seconds(10));
		reducedPair.print();
		

	}

	/**
	 * 
	 * @param timeMills
	 * @return
	 */
	public static String getDateHourString(String timeMills) {
		Timestamp ts = new Timestamp(new Long(timeMills));
		Date date = new Date();
		date = ts;

		return "" + date.getYear() + date.getMonth() + date.getDay() + date.getHours();
	}

	/***
	 * 
	 * @param propertyFile
	 * @return
	 * @throws Exception
	 */
	private static Properties loadProperties(String propertyFile) throws Exception {
		Properties prop = new Properties();
		InputStream input = new FileInputStream(propertyFile);
		prop.load(input);

		return prop;
	}

	public static void writeLineToFile(String fileName, String content) {
		FileWriter fw = null;
		try {
			File f = new File(fileName);
			fw = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);

		pw.println(content);
		pw.flush();

		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}