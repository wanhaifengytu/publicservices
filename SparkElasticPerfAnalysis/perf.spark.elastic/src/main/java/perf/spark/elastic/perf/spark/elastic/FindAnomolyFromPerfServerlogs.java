package perf.spark.elastic.perf.spark.elastic;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import static org.apache.spark.sql.functions.date_format;

import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.TimestampType;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.arrow.flatbuf.Type;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

//https://www.elastic.co/guide/en/elasticsearch/hadoop/master/spark.html#spark-sql-read-ds
public class FindAnomolyFromPerfServerlogs {

	public static void main(String[] args) throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.udf().register("generalizeURLUDF", new GeneralizeURLUDFPerflog(), DataTypes.StringType);
		sqlContext.udf().register("combineVectorColumns", new CombineVectorColumnsUDF(), DataTypes.createArrayType(DataTypes.DoubleType));
		sqlContext.udf().register("cleanColumnSpecialChars", new CleanColumnSpecialChars(), DataTypes.StringType);
		sqlContext.udf().register("concatArrayString", new ConcatArrayString(), DataTypes.StringType);
		
		sqlContext.udf().register("groupContactLogs", new GroupConcatColumnUDAF());

		String dateRange = (String) props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");

		Map options = new HashMap();
		options.put("pushdown", "true");
		options.put("es.nodes", (String) props.getProperty("esIPs"));
		options.put("es.port", (String) props.getProperty("esPort"));

		Dataset<Row> perflogsDataset = getPerflogsBySQL(sqlContext, options, dateRangeArray);
		Dataset<Row> serverLogs = getServerlogsBySQL(sqlContext, options, dateRangeArray);

		Dataset<Row> vectorServerlogs = word2Vec(serverLogs, "./word2vecmodel");

		Dataset<Row> perflogVectorDataset = combineVectorsByGid(perflogsDataset, vectorServerlogs);
		List<Row> list = findTopNGroups(perflogVectorDataset, 10);
		for (Row each: list) {
			calculateTopNKMeans(perflogVectorDataset, "vector", each.getString(0));
		}
		
		
		//saveToIndexTest(sqlContext, options);
		
	}
	
	public static void saveToIndexTest(SQLContext sqlContext, Map options) {
		//https://github.com/elastic/elasticsearch-hadoop/issues/1173
		//https://github.com/elastic/elasticsearch-hadoop/issues/1040#
		//
		Date start1 = new Date();
		Dataset<Row> datasetOrigin = sqlContext.read().format("org.elasticsearch.spark.sql").options(options)
				.load("monsoonperflogs").select(col("url"), col("ucpu"), col("mem"), col("timestamp"), col("gid"),
						col("sqlc"), col("sqlt"), col("rps"), col("rqt"));

		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		
		datasetOrigin = datasetOrigin.filter("rqt>3");
		//2019-11-18 05:45:01,350
		//datasetOrigin = datasetOrigin.withColumn("timestampNew", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss,SSS"));
		
		String esTimeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
		datasetOrigin = datasetOrigin.withColumn("timestampNew",date_format(to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss,SSS"), esTimeStampFormat));
		//datasetOrigin = datasetOrigin.withColumn("timestampNew", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss,SSS").cast(new TimestampType()));
		datasetOrigin.printSchema();
		datasetOrigin.show();
		
		JavaEsSparkSQL.saveToEs(datasetOrigin, "sparkanalysisperflog");
	}
	

	public static SparkSession getSparkSession(Properties props) throws Exception {
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

	public static Dataset<Row> getPerflogsBySQL(SQLContext sqlContext, Map options, String[] dateRangeArray) {
		Date start1 = new Date();
		Dataset<Row> datasetOrigin = sqlContext.read().format("org.elasticsearch.spark.sql").options(options)
				.load("monsoonperflogs").select(col("url"), col("ucpu"), col("mem"), col("timestamp"), col("gid"),
						col("sqlc"), col("sqlt"), col("rps"), col("rqt"));

		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		Date start21 = new Date();
		System.out.println("perf:::::::::::getPerflogs load data used time " + (start21.getTime() - start1.getTime()));

		Date start22 = new Date();
		Dataset<Row> datasetPerflogs = sqlContext.sql(
				"SELECT gid, url, ucpu, mem, timestamp, gid,sqlc, sqlt, rps, rqt  FROM monsoonperflogs monsoonperflogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid IS NOT NULL order by timestamp asc"); // and gid!=\"-\" and gid!=null
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsFiltered.csv", datasetPerflogs);
		// datasetPerflogs.count();
		datasetPerflogs.show();

		Date start23 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs filter data with sql time " + (start23.getTime() - start22.getTime()));

		Date start2 = new Date();
		datasetPerflogs = datasetPerflogs.withColumn("urlNew", callUDF("generalizeURLUDF", datasetPerflogs.col("url")));

		// datasetPerflogs.show();
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSampleUrlNew.csv", datasetPerflogs);
		Date start3 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs change column data used time " + (start3.getTime() - start2.getTime()));

		return datasetPerflogs;
	}

	public static Dataset<Row> getServerlogsBySQL(SQLContext sqlContext, Map options, String[] dateRangeArray) {
		Date start1 = new Date();
		Dataset<Row> datasetOrigin = sqlContext.read().format("org.elasticsearch.spark.sql").options(options)
				.load("monsoonserverlogs")
				.select(col("gid"), col("timestamp"), col("params"), col("logContent"), col("host.name").as("apphost"))
				.orderBy(col("timestamp").asc());
		datasetOrigin.createOrReplaceTempView("monsoonserverlogs");
		datasetOrigin.printSchema();
		datasetOrigin.show();

		Date start2 = new Date();
		System.out.println("perf:::::::::::getServerlogs load data used time " + (start2.getTime() - start1.getTime()));

		Date start21 = new Date();
		Dataset<Row> dataServerlogs = sqlContext
				.sql("SELECT  timestamp, gid, params, logContent FROM monsoonserverlogs monsoonserverlogs  where " // timestamp
																													// >
																													// \
						// + dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ " gid IS NOT NULL order by timestamp asc "); // and gid!=\"-\" and gid!=null
		dataServerlogs.show();
		CommonUtil.printTop5RowsOfDataset("monsoonserverflogsFiltered.csv", dataServerlogs);
		dataServerlogs.createOrReplaceTempView("monsoonserverlogsFilterd");
		Date start31 = new Date();
		System.out.println(
				"perf:::::::::::getServerlogs filter data used time " + (start31.getTime() - start21.getTime()));

		Date start3 = new Date();

		Dataset<Row> dataServerlogsGrp = sqlContext.sql(
				"SELECT gid, groupContactLogs(logContent) as allLogContents FROM monsoonserverlogsFilterd monsoonserverlogsFilterd group by gid");

		dataServerlogsGrp.show();
		CommonUtil.printTop5RowsOfDataset("monsoonserverflogsGrp.csv", dataServerlogsGrp);

		Date start4 = new Date();
		System.out.println("perf:::::::::::getServerlogs Grp data used time " + (start4.getTime() - start3.getTime()));
		return dataServerlogsGrp;
	}

	public static Dataset<Row> word2Vec(Dataset<Row> dataset, String savePath) throws Exception {
		StructType structType = new StructType();
		structType = structType.add("gid", DataTypes.StringType, true);
		structType = structType.add("text", DataTypes.createArrayType(DataTypes.StringType), true);

		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

		Dataset<Row> wordsDataset = dataset.map(row -> {
			String gid = (String) row.get(0);
			String logContents = (String) row.get(1);
			logContents = logContents.replaceAll("\\_", " ");
			logContents = logContents.replaceAll("\\,", " ");
			logContents = logContents.replaceAll("\\*", " ");

			String[] strs = logContents.split(" ");
			Row newRow = RowFactory.create(gid, strs);

			return newRow;
		}, encoder);

		// Learn a mapping from words to Vectors.
		Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(10).setMinCount(0);

		Word2VecModel model = word2Vec.fit(wordsDataset);
		// model.save(savePath);

		Dataset<Row> result = model.transform(wordsDataset);

		return result;
	}

	public static Dataset<Row> combineVectorsByGid(Dataset<Row> perflogDataset, Dataset<Row> serverlogVector) {
		Dataset<Row> perflogVectorData = SparkProcessLogs.joinTwoDatasets(perflogDataset, serverlogVector, "inner",
				"gid", "gid");
		// Normalize
		perflogVectorData = CommonUtil.normalizeRow(perflogVectorData, "ucpu");
		perflogVectorData = CommonUtil.normalizeRow(perflogVectorData, "mem");
		perflogVectorData = CommonUtil.normalizeRow(perflogVectorData, "rqt");

		perflogVectorData.show();

		// ucpu, mem, rqt, result
		perflogVectorData = perflogVectorData.withColumn("vector",
				callUDF("combineVectorColumns", perflogVectorData.col("ucpu"), perflogVectorData.col("mem"),
						perflogVectorData.col("rqt"), perflogVectorData.col("result")));
		//CommonUtil.printTop5RowsOfDataset("perflogwithserverlogvector.csv", perflogVectorData);
		
		for (int i = 0; i<13; i++) {
			perflogVectorData = perflogVectorData.withColumn("vector"+i,perflogVectorData.col("vector").getItem(i));
		}
		
		return perflogVectorData;
	}
	
	public static List<Row> findTopNGroups(Dataset<Row> perflogVectorData, int topN) {
		Dataset<Row> groupedData = perflogVectorData.groupBy(perflogVectorData.col("url")).agg(avg(perflogVectorData.col("rqt")).as("avgrqt"));
		groupedData = groupedData.orderBy(groupedData.col("avgrqt").desc());
		
		groupedData.show();
		CommonUtil.printTop5RowsOfDataset("perflogGroupByURLdescRqt.csv", groupedData);
		
		List<Row> listTopN = groupedData.takeAsList(topN);
		
		return listTopN;
	}

	public static void calculateTopNKMeans(Dataset<Row> dataSet, String vectorColumn, String url) {
		Dataset<Row>  trainingData = dataSet.filter(dataSet.col("url").equalTo(url));
		
		String[] inputCols = {"vector0", "vector0","vector2","vector3","vector4","vector5","vector6","vector7","vector8", "vector9","vector10","vector11", "vector12"};
		VectorAssembler assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")
				.setHandleInvalid("skip");
		
		KMeans kmeans = new KMeans().setK(2).setMaxIter(50).setFeaturesCol("featureVector").setPredictionCol("prediction");
		
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { assembler, kmeans });
		PipelineModel pipelineModel = pipeline.fit(trainingData);

		Transformer[] allTransformers = pipelineModel.stages();
		Transformer lastTransformer = allTransformers[allTransformers.length - 1];
		KMeansModel kmeansModel = (KMeansModel) lastTransformer;

		Dataset<Row> transformedData = assembler.transform(trainingData);
		//transformedData.show();
		
		Dataset<Row> transDataset = pipelineModel.transform(trainingData);
		String esTimeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
		transDataset = transDataset.withColumn("timestampNew",date_format(to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss,SSS"), esTimeStampFormat));
		
		transDataset.printSchema();
		
		String fileName = url.replaceAll("\\/", "");
		if (fileName.length() > 15) {
			fileName = fileName.substring(0,15);
		}
		CommonUtil.printAllRowsOfDataset(fileName  + "clustering.csv",transDataset);
		
		transDataset = transDataset.select(col("monsoonperflogs.gid").as("gid"),col("ucpu").cast("string"), col("mem").cast("string"), col("rqt").cast("string"),
						col("url"), col("sqlc"), col("sqlt"), col("rps"), col("urlNew"), col("text"), col("vector"), col("prediction"), col("timestampNew"))
				        .withColumn("textLogs", callUDF("concatArrayString", col("text"))).drop(col("text"));;
		
		transDataset.printSchema();
		transDataset.show();
		
		JavaEsSparkSQL.saveToEs(transDataset, "perflogserverlogclustering");
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

	// From original spark query
	public static Dataset<Row> getPerflogs(SparkSession sparkSession, String[] dateRangeArray) {
		Date start1 = new Date();
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs")
				.select(col("url"), col("ucpu"), col("mem"), col("timestamp"), col("gid"), col("sqlc"), col("sqlt"),
						col("rps"), col("rqt"));

		datasetOrigin.printSchema();
		// datasetOrigin.show();
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		Date start21 = new Date();
		System.out.println("perf:::::::::::getPerflogs load data used time " + (start21.getTime() - start1.getTime()));

		Date start22 = new Date();
		Dataset<Row> datasetPerflogs = sparkSession.sql(
				"SELECT url, ucpu, mem, timestamp, gid,sqlc, sqlt, rps, rqt  FROM monsoonperflogs monsoonperflogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid IS NOT NULL order by timestamp asc"); // and gid!=\"-\" and gid!=null
		// CommonUtil.printTop5RowsOfDataset("monsoonperflogsSample.csv",
		// datasetPerflogs);
		// datasetPerflogs.count();
		// datasetPerflogs.show();

		Date start23 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs filter data with sql time " + (start23.getTime() - start22.getTime()));

		Date start2 = new Date();
		datasetPerflogs = datasetPerflogs.withColumn("urlNew", callUDF("generalizeURLUDF", datasetPerflogs.col("url")));

		// datasetPerflogs.show();
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSampleUrlNew.csv", datasetPerflogs);
		Date start3 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs change column data used time " + (start3.getTime() - start2.getTime()));

		return datasetPerflogs;
	}

	public static Dataset<Row> getServerlogs(SparkSession sparkSession, String[] dateRangeArray) {
		Date start1 = new Date();
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonserverlogs")
				.select(col("gid"), col("timestamp"), col("params"), col("logContent"), col("host.name").as("apphost"))
				.orderBy(col("timestamp").asc());
		Date start2 = new Date();
		System.out.println("perf:::::::::::getServerlogs load data used time " + (start2.getTime() - start1.getTime()));
		datasetOrigin.createOrReplaceTempView("monsoonserverlogs");
		datasetOrigin.printSchema();

		Date start21 = new Date();
		Dataset<Row> dataServerlogs = sparkSession.sql(
				"SELECT  timestamp, gid, params, logContent FROM monsoonserverlogs monsoonserverlogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid IS NOT NULL order by timestamp asc "); // and gid!=\"-\" and gid!=null
		// dataServerlogs.show(); =>Very time consuming operation !!!
		CommonUtil.printTop5RowsOfDataset("monsoonserverflogsFiltered.csv", dataServerlogs);
		dataServerlogs.createOrReplaceTempView("monsoonserverlogsFilterd");
		Date start31 = new Date();
		System.out.println(
				"perf:::::::::::getServerlogs select again data used time " + (start31.getTime() - start21.getTime()));

		Date start3 = new Date();

		Dataset<Row> dataServerlogsGrp = sparkSession.sql(
				"SELECT gid, groupContactLogs(logContent) as allLogContents FROM monsoonserverlogsFilterd monsoonserverlogsFilterd group by gid");

		// dataServerlogs.show();
		CommonUtil.printTop5RowsOfDataset("monsoonserverflogsGrp.csv", dataServerlogsGrp);

		Date start4 = new Date();
		System.out.println("perf:::::::::::getServerlogs load data used time " + (start4.getTime() - start3.getTime()));
		return dataServerlogsGrp;
	}
}
