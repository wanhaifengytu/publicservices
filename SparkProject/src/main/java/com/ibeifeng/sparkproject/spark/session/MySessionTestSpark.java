package com.ibeifeng.sparkproject.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.dao.factory.DAOFactory;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.util.DateUtils;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import com.ibeifeng.sparkproject.util.StringUtils;
import com.ibeifeng.sparkproject.util.ValidUtils;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import scala.Tuple2;

public class MySessionTestSpark {
	public static void main(String[] args) {
		// Create Spark Context
		SparkConf conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
				.set("spark.storage.memoryFraction", "0.5").set("spark.shuffle.consolidateFiles", "true")
				.set("spark.shuffle.file.buffer", "64").set("spark.shuffle.memoryFraction", "0.3")
				.set("spark.reducer.maxSizeInFlight", "24").set("spark.shuffle.io.maxRetries", "60")
				.set("spark.shuffle.io.retryWait", "60")
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.registerKryoClasses(new Class[] { CategorySortKey.class, IntList.class });
		SparkUtils.setMaster(conf);

		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = getSQLContext(sc.sc());

		SparkUtils.mockData(sc, sqlContext);

		// DAO to get task information
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		long taskid = 2;
		Task task = taskDAO.findById(taskid);
		if (task == null) {
			System.out.println(new Date() + ": cannot find this task with id [" + taskid + "].");
			return;
		} else {
			System.out
					.println("Found Task " + task.getTaskid() + " " + task.getTaskName() + "  " + task.getTaskParam());
		}
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		String sql = "select * " + "from user_visit_action " + "where date>='" + startDate + "' " + "and date<='"
				+ endDate + "'";
		System.out.println("Execute SQL " + sql);
		DataFrame actionDF = sqlContext.sql(sql);

		JavaRDD<Row> filteredRows = actionDF.javaRDD();
		System.out.println("Got record number " + filteredRows.count());

		// Map to actions->Row pair
		JavaPairRDD<String, Row> actionsPair = filteredRows
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {
					@Override
					public Iterable<Tuple2<String, Row>> call(Iterator<Row> rowIter) throws Exception {
						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
						while (rowIter.hasNext()) {
							Row row = rowIter.next();
							list.add(new Tuple2<String, Row>(row.getString(2), row));
						}
						return list;
					}
				});

		// Persist
		actionsPair = actionsPair.persist(StorageLevel.MEMORY_ONLY());

		JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(sc, sqlContext, actionsPair);

		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(sessionid2AggrInfoRDD,
				taskParam, sessionAggrStatAccumulator);

		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());

		JavaPairRDD<String, Row> sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD,
				actionsPair);
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());

		randomExtractSession(sc, task.getTaskid(), filteredSessionid2AggrInfoRDD, sessionid2detailRDD);

		calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskid());

		// Got top10 products categories
		List<Tuple2<CategorySortKey, String>> top10CategoryList = getTop10Category(task.getTaskid(),
				sessionid2detailRDD);

		// Got top10 active sessions
		getTop10Session(sc, task.getTaskid(), top10CategoryList, sessionid2detailRDD);
		
		sc.close();
	}

	/**
	 * Get top10 active session
	 * 
	 * @param sc
	 * @param taskid
	 * @param top10CategoryList
	 * @param sessionid2detailRDD
	 */
	private static void getTop10Session(JavaSparkContext sc, final long taskid,
			List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionid2detailRDD) {

		List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();
		// Loop over top10 category list, extract the categoryId and add to list
		for (Tuple2<CategorySortKey, String> category : top10CategoryList) {
			long categoryid = Long
					.valueOf(StringUtils.getFieldFromConcatString(category._2, "\\|", Constants.FIELD_CATEGORY_ID));
			top10CategoryIdList.add(new Tuple2<Long, Long>(categoryid, categoryid));
		}

		final JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);

		// Group the session => sessionID, Iterable(row of actionDetails)
		JavaPairRDD<String, Iterable<Row>> sessionid2detailsRDD = sessionid2detailRDD.groupByKey();

		final JavaPairRDD<Long, String> categoryid2sessionCountRDD = sessionid2detailsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
					@Override
					public Iterable<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionid = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();

						Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();
						while (iterator.hasNext()) {
							Row row = iterator.next();
							if (row.get(6) != null) {
								long categoryid = row.getLong(6);
								Long count = categoryCountMap.get(categoryid);
								if (count == null) {
									count = 0L;
								}
								count++;
								categoryCountMap.put(categoryid, count);
							}
						}
						// Return categoryId sessionId,count
						List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();
						for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
							long categoryid = categoryCountEntry.getKey();
							long count = categoryCountEntry.getValue();
							String value = sessionid + "," + count;
							list.add(new Tuple2<Long, String>(categoryid, value));
						}
						return list;
					}
				}) ;

					JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
							.join(categoryid2sessionCountRDD)
							.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
								@Override
								public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple)
										throws Exception {
									return new Tuple2<Long, String>(tuple._1, tuple._2._2);
								}
							});

					JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD = top10CategorySessionCountRDD
							.groupByKey();

					JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD
							.flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
								private static final long serialVersionUID = 1L;
								@Override
								public Iterable<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple)
										throws Exception {
									long categoryid = tuple._1;
									Iterator<String> iterator = tuple._2.iterator();
									
									String[] top10Sessions = new String[10]; 
									while(iterator.hasNext()) {
										String sessionCount = iterator.next();
										long count = Long.valueOf(sessionCount.split(",")[1]);  
										
										// ??????????????????
										for(int i = 0; i < top10Sessions.length; i++) {
											// ????????????i????????????????????????????????????i????????????????????????sessionCount
											if(top10Sessions[i] == null) {
												top10Sessions[i] = sessionCount;
												break;
											} else {
												long _count = Long.valueOf(top10Sessions[i].split(",")[1]);  
												
												// ??????sessionCount???i??????sessionCount??????
												if(count > _count) {
													// ???????????????????????????????????????i?????????????????????????????????
													for(int j = 9; j > i; j--) {
														top10Sessions[j] = top10Sessions[j - 1];
													}
													// ???i????????????sessionCount
													top10Sessions[i] = sessionCount;
													break;
												}			
												// ????????????????????????for??????
											}
										}
									}
									
									List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
									
									for(String sessionCount : top10Sessions) {
										if(sessionCount != null) {
											String sessionid = sessionCount.split(",")[0];
											long count = Long.valueOf(sessionCount.split(",")[1]);  
											
											// ???top10 session??????MySQL???
											Top10Session top10Session = new Top10Session();
											top10Session.setTaskid(taskid);  
											top10Session.setCategoryid(categoryid);  
											top10Session.setSessionid(sessionid);  
											top10Session.setClickCount(count);  
											
											ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();
											top10SessionDAO.insert(top10Session);  
											
											// ??????list
											list.add(new Tuple2<String, String>(sessionid, sessionid));
										}
									}
									
									return list;
								}
							});

					JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
							top10SessionRDD.join(sessionid2detailRDD);  
					sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>() {  
						private static final long serialVersionUID = 1L;
						@Override
						public void call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
							Row row = tuple._2._2;	
							SessionDetail sessionDetail = new SessionDetail();
							sessionDetail.setTaskid(taskid);  
							sessionDetail.setUserid(row.getLong(1));  
							sessionDetail.setSessionid(row.getString(2));  
							sessionDetail.setPageid(row.getLong(3));  
							sessionDetail.setActionTime(row.getString(4));
							sessionDetail.setSearchKeyword(row.getString(5));  
							sessionDetail.setClickCategoryId(row.getLong(6));  
							sessionDetail.setClickProductId(row.getLong(7));   
							sessionDetail.setOrderCategoryIds(row.getString(8));  
							sessionDetail.setOrderProductIds(row.getString(9));  
							sessionDetail.setPayCategoryIds(row.getString(10)); 
							sessionDetail.setPayProductIds(row.getString(11));  
							
							ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
							sessionDetailDAO.insert(sessionDetail);  
						}
					});
	}

	/**
	 * Input sessionid2detailRDD JavaPairRDD<String, Row> =>
	 * 
	 * Output List<Tuple2<CategorySortKey, String>> =>
	 * 
	 * 
	 * @param taskId
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static List<Tuple2<CategorySortKey, String>> getTop10Category(long taskId,
			JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<Long, Long> categoryidRDD = sessionid2detailRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {

						Row row = tuple._2;
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						Long clickCategoryId = row.getLong(6);
						if (clickCategoryId != null) {
							list.add(new Tuple2<Long, Long>(clickCategoryId, clickCategoryId));
						}
						// User may order a few categories at one time
						String orderCategoryIds = row.getString(8);
						if (orderCategoryIds != null) {
							String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
							for (String orderCategoryId : orderCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId),
										Long.valueOf(orderCategoryId)));
							}
						}

						// User may pay multiple categorized at one time.
						// separated by ,
						String payCategoryIds = row.getString(10);
						if (payCategoryIds != null) {
							String[] payCategoryIdsSplited = payCategoryIds.split(",");
							for (String payCategoryId : payCategoryIdsSplited) {
								list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId),
										Long.valueOf(payCategoryId)));
							}
						}
						return list;
					}
				});

		// Remove the duplicate elements
		categoryidRDD.distinct();

		// Get every category click count
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD);
		// Get every category order count
		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD);
		// Get every category pay count
		JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD);

		JavaPairRDD<Long, String> categoryid2countRDD = joinCategoryAndData(categoryidRDD, clickCategoryId2CountRDD,
				orderCategoryId2CountRDD, payCategoryId2CountRDD);

		JavaPairRDD<CategorySortKey, String> sortKey2countRDD = categoryid2countRDD
				.mapToPair(new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
					@Override
					public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple) throws Exception {
						String countInfo = tuple._2;
						long clickCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
						long orderCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
						long payCount = Long.valueOf(
								StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));
						CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);
						return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
					}
				});

		JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false);

		ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();

		List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);

		// Write to mysql
		for (Tuple2<CategorySortKey, String> tuple : top10CategoryList) {
			String countInfo = tuple._2;
			long categoryid = Long
					.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
			long clickCount = Long
					.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
			long orderCount = Long
					.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
			long payCount = Long
					.valueOf(StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT));

			Top10Category category = new Top10Category();
			category.setTaskid(taskId);
			category.setCategoryid(categoryid);
			category.setClickCount(clickCount);
			category.setOrderCount(orderCount);
			category.setPayCount(payCount);

			top10CategoryDAO.insert(category);
		}

		return top10CategoryList;
	}

	/**
	 * 
	 * @param categoryidRDD
	 *            =====categoryid(click or order or pay) , same as former.
	 * @param clickCategoryId2CountRDD
	 *            === categoryId clickCount
	 * @param orderCategoryId2CountRDD
	 *            ====categoryId orderCount
	 * @param payCategoryId2CountRDD
	 *            =====categoryId payCount
	 * @return ===== categoryId
	 *         categoryId=xxx|clickCount=xxx|orderCount=xxx|payCount=xxxx
	 */
	private static JavaPairRDD<Long, String> joinCategoryAndData(JavaPairRDD<Long, Long> categoryidRDD,
			JavaPairRDD<Long, Long> clickCategoryId2CountRDD, JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
			JavaPairRDD<Long, Long> payCategoryId2CountRDD) {
		// With leftOuterJoin, then second value may not be present, so use
		// optional
		JavaPairRDD<Long, Tuple2<Long, Optional<Long>>> tmpJoinRDD = categoryidRDD
				.leftOuterJoin(clickCategoryId2CountRDD);

		// Convert to a kind of format: categoryId=xxx|clickCount=xxx
		JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						Optional<Long> optional = tuple._2._2;
						long clickCount = 0L;

						if (optional.isPresent()) {
							clickCount = optional.get();
						}
						String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|"
								+ Constants.FIELD_CLICK_COUNT + "=" + clickCount;
						return new Tuple2<Long, String>(categoryid, value);
					}
				});

		tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD)
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;

						Optional<Long> optional = tuple._2._2;
						long orderCount = 0L;
						if (optional.isPresent()) {
							orderCount = optional.get();
						}
						value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;
						return new Tuple2<Long, String>(categoryid, value);
					}
				});

		tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(

				new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple)
							throws Exception {
						long categoryid = tuple._1;
						String value = tuple._2._1;

						Optional<Long> optional = tuple._2._2;
						long payCount = 0L;

						if (optional.isPresent()) {
							payCount = optional.get();
						}

						value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

						return new Tuple2<Long, String>(categoryid, value);
					}

				});

		return tmpMapRDD;
	}

	/**
	 * Get every category clickCount
	 * 
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {

		/**
		 * Filter out that only clicked.
		 */
		JavaPairRDD<String, Row> clickActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.get(6) != null ? true : false;
					}
				});

		/**
		 * Map the filtered session to category ID pair
		 */
		JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD
				.mapToPair(new PairFunction<Tuple2<String, Row>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Long> call(Tuple2<String, Row> tuple) throws Exception {
						long clickCategoryId = tuple._2.getLong(6);
						return new Tuple2<Long, Long>(clickCategoryId, 1L);
					}
				});

		/**
		 * 
		 */
		JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD
				.reduceByKey(new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		return clickCategoryId2CountRDD;
	}

	/**
	 * Get order category Count RDD => (orderedCategory , count)
	 * 
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> orderActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(8) != null ? true : false;
					}
				});

		JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String orderCategoryIds = row.getString(8);
						String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for (String orderCategoryId : orderCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
						}
						return list;
					}
				});

		JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(

				new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		return orderCategoryId2CountRDD;
	}

	/**
	 * 
	 * @param sessionid2detailRDD
	 * @return
	 */
	private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2detailRDD) {
		JavaPairRDD<String, Row> payActionRDD = sessionid2detailRDD
				.filter(new Function<Tuple2<String, Row>, Boolean>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						return row.getString(10) != null ? true : false;
					}
				});

		JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(

				new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
						Row row = tuple._2;
						String payCategoryIds = row.getString(10);
						String[] payCategoryIdsSplited = payCategoryIds.split(",");

						List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
						for (String payCategoryId : payCategoryIdsSplited) {
							list.add(new Tuple2<Long, Long>(Long.valueOf(payCategoryId), 1L));
						}
						return list;
					}
				});

		JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(

				new Function2<Long, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Long call(Long v1, Long v2) throws Exception {
						return v1 + v2;
					}
				});

		return payCategoryId2CountRDD;
	}

	private static void calculateAndPersistAggrStat(String value, long taskid) {
		// ???Accumulator?????????????????????
		long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

		long visit_length_1s_3s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
		long visit_length_4s_6s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
		long visit_length_7s_9s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
		long visit_length_10s_30s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
		long visit_length_30s_60s = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
		long visit_length_1m_3m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
		long visit_length_3m_10m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m));
		long visit_length_10m_30m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
		long visit_length_30m = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

		long step_length_1_3 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
		long step_length_4_6 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
		long step_length_7_9 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
		long step_length_10_30 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
		long step_length_30_60 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
		long step_length_60 = Long
				.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

		// ????????????????????????????????????????????????
		double visit_length_1s_3s_ratio = NumberUtils.formatDouble((double) visit_length_1s_3s / (double) session_count,
				2);
		double visit_length_4s_6s_ratio = NumberUtils.formatDouble((double) visit_length_4s_6s / (double) session_count,
				2);
		double visit_length_7s_9s_ratio = NumberUtils.formatDouble((double) visit_length_7s_9s / (double) session_count,
				2);
		double visit_length_10s_30s_ratio = NumberUtils
				.formatDouble((double) visit_length_10s_30s / (double) session_count, 2);
		double visit_length_30s_60s_ratio = NumberUtils
				.formatDouble((double) visit_length_30s_60s / (double) session_count, 2);
		double visit_length_1m_3m_ratio = NumberUtils.formatDouble((double) visit_length_1m_3m / (double) session_count,
				2);
		double visit_length_3m_10m_ratio = NumberUtils
				.formatDouble((double) visit_length_3m_10m / (double) session_count, 2);
		double visit_length_10m_30m_ratio = NumberUtils
				.formatDouble((double) visit_length_10m_30m / (double) session_count, 2);
		double visit_length_30m_ratio = NumberUtils.formatDouble((double) visit_length_30m / (double) session_count, 2);

		double step_length_1_3_ratio = NumberUtils.formatDouble((double) step_length_1_3 / (double) session_count, 2);
		double step_length_4_6_ratio = NumberUtils.formatDouble((double) step_length_4_6 / (double) session_count, 2);
		double step_length_7_9_ratio = NumberUtils.formatDouble((double) step_length_7_9 / (double) session_count, 2);
		double step_length_10_30_ratio = NumberUtils.formatDouble((double) step_length_10_30 / (double) session_count,
				2);
		double step_length_30_60_ratio = NumberUtils.formatDouble((double) step_length_30_60 / (double) session_count,
				2);
		double step_length_60_ratio = NumberUtils.formatDouble((double) step_length_60 / (double) session_count, 2);

		// ????????????????????????Domain??????
		SessionAggrStat sessionAggrStat = new SessionAggrStat();
		sessionAggrStat.setTaskid(taskid);
		sessionAggrStat.setSession_count(session_count);
		sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
		sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
		sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
		sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
		sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
		sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
		sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
		sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
		sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
		sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
		sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
		sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
		sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
		sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
		sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

		// ???????????????DAO??????????????????
		ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
		sessionAggrStatDAO.insert(sessionAggrStat);

	}

	private static void randomExtractSession(JavaSparkContext sc, final long taskid,
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, JavaPairRDD<String, Row> sessionid2actionRDD) {
		// ??????<yyyy-MM-dd_HH,aggrInfo>?????????RDD
		JavaPairRDD<String, String> time2sessionidRDD = sessionid2AggrInfoRDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<String, String> tuple) throws Exception {
						String aggrInfo = tuple._2;
						String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|",
								Constants.FIELD_START_TIME);
						String dateHour = DateUtils.getDateHour(startTime);
						return new Tuple2<String, String>(dateHour, aggrInfo);
					}
				});

		Map<String, Object> countMap = time2sessionidRDD.countByKey();

		// ???<yyyy-MM-dd_HH,count>?????????map????????????<yyyy-MM-dd,<HH,count>>?????????
		Map<String, Map<String, Long>> dateHourCountMap = new HashMap<String, Map<String, Long>>();

		for (Map.Entry<String, Object> countEntry : countMap.entrySet()) {
			String dateHour = countEntry.getKey();
			String date = dateHour.split("_")[0];
			String hour = dateHour.split("_")[1];
			long count = Long.valueOf(String.valueOf(countEntry.getValue()));

			Map<String, Long> hourCountMap = dateHourCountMap.get(date);
			if (hourCountMap == null) {
				hourCountMap = new HashMap<String, Long>();
				dateHourCountMap.put(date, hourCountMap);
			}

			hourCountMap.put(hour, count);
		}

		// ???????????????100???session?????????????????????????????????
		int bDivided = dateHourCountMap.size();
		if (dateHourCountMap.size() == 0) {
			bDivided = 1;
		}
		int extractNumberPerDay = 100 / bDivided;
		Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
		Random random = new Random();

		for (Map.Entry<String, Map<String, Long>> dateHourCountEntry : dateHourCountMap.entrySet()) {
			String date = dateHourCountEntry.getKey();
			Map<String, Long> hourCountMap = dateHourCountEntry.getValue();

			// ?????????????????????session??????
			long sessionCount = 0L;
			for (long hourCount : hourCountMap.values()) {
				sessionCount += hourCount;
			}
			Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
			if (hourExtractMap == null) {
				hourExtractMap = new HashMap<String, List<Integer>>();
				dateHourExtractMap.put(date, hourExtractMap);
			}

			// ??????????????????
			for (Map.Entry<String, Long> hourCountEntry : hourCountMap.entrySet()) {
				String hour = hourCountEntry.getKey();
				long count = hourCountEntry.getValue();

				// ?????????????????????session????????????????????????session??????????????????????????????????????????????????????
				// ????????????????????????????????????????????????session??????
				int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
				if (hourExtractNumber > count) {
					hourExtractNumber = (int) count;
				}

				// ??????????????????????????????????????????list
				List<Integer> extractIndexList = hourExtractMap.get(hour);
				if (extractIndexList == null) {
					extractIndexList = new ArrayList<Integer>();
					hourExtractMap.put(hour, extractIndexList);
				}

				// ?????????????????????????????????????????????
				for (int i = 0; i < hourExtractNumber; i++) {
					int extractIndex = random.nextInt((int) count);
					while (extractIndexList.contains(extractIndex)) {
						extractIndex = random.nextInt((int) count);
					}
					extractIndexList.add(extractIndex);
				}
			}
		}

		Map<String, Map<String, IntList>> fastutilDateHourExtractMap = new HashMap<String, Map<String, IntList>>();

		for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry : dateHourExtractMap.entrySet()) {
			String date = dateHourExtractEntry.getKey();
			Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
			Map<String, IntList> fastutilHourExtractMap = new HashMap<String, IntList>();
			for (Map.Entry<String, List<Integer>> hourExtractEntry : hourExtractMap.entrySet()) {
				String hour = hourExtractEntry.getKey();
				List<Integer> extractList = hourExtractEntry.getValue();
				IntList fastutilExtractList = new IntArrayList();
				for (int i = 0; i < extractList.size(); i++) {
					fastutilExtractList.add(extractList.get(i));
				}
				fastutilHourExtractMap.put(hour, fastutilExtractList);
			}
			fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
		}

		/**
		 * BroadCast variable.. just new it..
		 */
		final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast = sc
				.broadcast(fastutilDateHourExtractMap);
		// ??????groupByKey???????????????<dateHour,(session aggrInfo)>
		JavaPairRDD<String, Iterable<String>> time2sessionsRDD = time2sessionidRDD.groupByKey();

		JavaPairRDD<String, String> extractSessionidsRDD = time2sessionsRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple)
							throws Exception {
						List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();
						String dateHour = tuple._1;
						String date = dateHour.split("_")[0];
						String hour = dateHour.split("_")[1];
						Iterator<String> iterator = tuple._2.iterator();

						Map<String, Map<String, IntList>> dateHourExtractMap = dateHourExtractMapBroadcast.value();
						List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);

						ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

						int index = 0;
						while (iterator.hasNext()) {
							String sessionAggrInfo = iterator.next();
							if (extractIndexList.contains(index)) {
								String sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo, "\\|",
										Constants.FIELD_SESSION_ID);

								// ???????????????MySQL
								SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();
								sessionRandomExtract.setTaskid(taskid);
								sessionRandomExtract.setSessionid(sessionid);
								sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,
										"\\|", Constants.FIELD_START_TIME));
								sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
								sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
										sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));
								sessionRandomExtractDAO.insert(sessionRandomExtract);

								// ???sessionid??????list
								extractSessionids.add(new Tuple2<String, String>(sessionid, sessionid));
							}
							index++;
						}
						return extractSessionids;
					}

				});
		// Get the details through a join
		JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD = extractSessionidsRDD
				.join(sessionid2actionRDD);

		extractSessionDetailRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
			@Override
			public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
				List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();
				while (iterator.hasNext()) {
					Tuple2<String, Tuple2<String, Row>> tuple = iterator.next();
					Row row = tuple._2._2;

					SessionDetail sessionDetail = new SessionDetail();
					sessionDetail.setTaskid(taskid);
					sessionDetail.setUserid(row.getLong(1));
					sessionDetail.setSessionid(row.getString(2));
					sessionDetail.setPageid(row.getLong(3));
					sessionDetail.setActionTime(row.getString(4));
					sessionDetail.setSearchKeyword(row.getString(5));
					sessionDetail.setClickCategoryId(row.getLong(6));
					sessionDetail.setClickProductId(row.getLong(7));
					sessionDetail.setOrderCategoryIds(row.getString(8));
					sessionDetail.setOrderProductIds(row.getString(9));
					sessionDetail.setPayCategoryIds(row.getString(10));
					sessionDetail.setPayProductIds(row.getString(11));

					sessionDetails.add(sessionDetail);
				}
				ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
				sessionDetailDAO.insertBatch(sessionDetails);
			}
		});

	}

	private static JavaPairRDD<String, Row> getSessionid2detailRDD(JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD) {
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
					@Override
					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
				});

		return sessionid2detailRDD;
	}

	/***
	 * 
	 * @param sessionid2AggrInfoRDD
	 * @param taskParam
	 * @param sessionAggrStatAccumulator
	 * @return
	 */
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator) {

		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				+ (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				+ (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
				+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
				+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
				+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
				+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds : "");

		System.out.println(" Filter Parameter " + _parameter);

		if (_parameter.endsWith("\\|")) {
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		final String parameter = _parameter;

		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD
				.filter(new Function<Tuple2<String, String>, Boolean>() {

					@Override
					public Boolean call(Tuple2<String, String> tuple) throws Exception {
						// ????????????tuple????????????????????????
						String aggrInfo = tuple._2;

						// ?????????????????????????????????????????????
						// ?????????????????????????????????startAge???endAge???
						if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter, Constants.PARAM_START_AGE,
								Constants.PARAM_END_AGE)) {
							return false;
						}

						// ?????????????????????????????????professionals???
						// ?????????,IT,??????
						// ?????????
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
								Constants.PARAM_PROFESSIONALS)) {
							return false;
						}

						// ?????????????????????????????????cities???
						// ??????,??????,??????,??????
						// ??????
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)) {
							return false;
						}

						// ????????????????????????
						// ???/???
						// ?????????
						if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)) {
							return false;
						}

						// ???????????????????????????
						// ?????????session??????????????? ??????,??????,??????
						// ?????????????????????????????? ??????,?????????,iphone??????
						// ?????????in?????????????????????????????????session??????????????????????????????????????????????????????
						// ???????????????????????????????????????
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
								Constants.PARAM_KEYWORDS)) {
							return false;
						}

						// ??????????????????id????????????
						if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
								Constants.PARAM_CATEGORY_IDS)) {
							return false;
						}

						// ???????????????????????????????????????????????????????????????????????????
						// ?????????????????????session?????????????????????????????????????????????????????????????????????session
						// ???????????????session??????????????????????????????????????????????????????session???????????????
						// ???????????????????????????

						// ???????????????????????????????????????????????????session
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

						// ?????????session??????????????????????????????????????????????????????????????????
						long visitLength = Long.valueOf(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
						long stepLength = Long.valueOf(
								StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));
						calculateVisitLength(visitLength);
						calculateStepLength(stepLength);

						return true;
					}

					/**
					 * ????????????????????????
					 * 
					 * @param visitLength
					 */
					private void calculateVisitLength(long visitLength) {
						if (visitLength >= 1 && visitLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
						} else if (visitLength >= 4 && visitLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
						} else if (visitLength >= 7 && visitLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
						} else if (visitLength >= 10 && visitLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
						} else if (visitLength > 30 && visitLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
						} else if (visitLength > 60 && visitLength <= 180) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
						} else if (visitLength > 180 && visitLength <= 600) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
						} else if (visitLength > 600 && visitLength <= 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
						} else if (visitLength > 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
						}
					}

					/**
					 * ????????????????????????
					 * 
					 * @param stepLength
					 */
					private void calculateStepLength(long stepLength) {
						if (stepLength >= 1 && stepLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
						} else if (stepLength >= 4 && stepLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
						} else if (stepLength >= 7 && stepLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
						} else if (stepLength >= 10 && stepLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
						} else if (stepLength > 30 && stepLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
						} else if (stepLength > 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
						}
					}
				});

		return filteredSessionid2AggrInfoRDD;
	}

	/**
	 * 
	 * @param actionsPair
	 * @return
	 */
	private static JavaPairRDD<String, String> aggregateBySession(JavaSparkContext sc, SQLContext sqlContext,
			JavaPairRDD<String, Row> actionsPair) {
		// Group by sessionId which is also the key
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = actionsPair.groupByKey();

		// Map to pair. map to needed format.
		// original format session id == Iterable<Row>
		// dest format user id == String with necessary information
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD
				.mapToPair(new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
					@Override
					public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
						String sessionId = tuple._1;
						Iterator<Row> iterator = tuple._2.iterator();

						StringBuffer searchKeywordsBuffer = new StringBuffer("");
						StringBuffer clickCategoryIdsBuffer = new StringBuffer("");

						Long userid = null;

						Date startTime = null;
						Date endTime = null;
						// session???????????????
						int stepLength = 0;

						// ??????session?????????????????????
						while (iterator.hasNext()) {
							// ???????????????????????????????????????????????????????????????
							Row row = iterator.next();
							if (userid == null) {
								userid = row.getLong(1);
							}
							String searchKeyword = row.getString(5);
							Long clickCategoryId = row.getLong(6);

							if (StringUtils.isNotEmpty(searchKeyword)) {
								if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
									searchKeywordsBuffer.append(searchKeyword + ",");
								}
							}
							if (clickCategoryId != null) {
								if (!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))) {
									clickCategoryIdsBuffer.append(clickCategoryId + ",");
								}
							}

							// ??????session?????????????????????
							Date actionTime = DateUtils.parseTime(row.getString(4));

							if (startTime == null) {
								startTime = actionTime;
							}
							if (endTime == null) {
								endTime = actionTime;
							}

							if (actionTime.before(startTime)) {
								startTime = actionTime;
							}
							if (actionTime.after(endTime)) {
								endTime = actionTime;
							}

							// ??????session????????????
							stepLength++;
						}

						String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
						String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());

						// ??????session?????????????????????
						long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
						String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|"
								+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
								+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
								+ Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH
								+ "=" + stepLength + "|" + Constants.FIELD_START_TIME + "="
								+ DateUtils.formatTime(startTime);
						return new Tuple2<Long, String>(userid, partAggrInfo);
					}
				});

		String sql = "select * from user_info";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();

		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
			@Override
			public Tuple2<Long, Row> call(Row row) throws Exception {
				return new Tuple2<Long, Row>(row.getLong(0), row);
			}
		});

		// Then join them
		// ???session??????????????????????????????????????????join
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

		// ???join??????????????????????????????????????????<sessionid,fullAggrInfo>???????????????
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD
				.mapToPair(new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
					@Override
					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|",
								Constants.FIELD_SESSION_ID);

						int age = userInfoRow.getInt(3);
						String professional = userInfoRow.getString(4);
						String city = userInfoRow.getString(5);
						String sex = userInfoRow.getString(6);

						String fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "="
								+ city + "|" + Constants.FIELD_SEX + "=" + sex;

						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
				});

		return sessionid2FullAggrInfoRDD;
	}

	private static SQLContext getSQLContext(SparkContext sc) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if (local) {
			return new SQLContext(sc);
		} else {
			return new HiveContext(sc);
		}
	}
}
