package com.spark.analysis.logs;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class FindAnomolyFromPerflogsServerLogs {

	public static void main(String[] args) throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.udf().register("generalizeURLUDF", new GeneralizeURLUDFPerflog(), DataTypes.StringType);
		sqlContext.udf().register("groupContactLogs", new GroupConcatColumnUDAF());

		String dateRange = (String) props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");

		Dataset<Row> perflogDataset = getPerflogs(sparkSession, dateRangeArray);
		Dataset<Row> serverLogs = getServerlogs(sparkSession, dateRangeArray);

		//joinPerfAndServerlogs(perflogDataset, serverLogs);
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

	public static Dataset<Row> getPerflogs(SparkSession sparkSession, String[] dateRangeArray) {
		Date start1 =new Date();
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs")
				.select(col("url"), col("ucpu"), col("mem"), col("timestamp"), col("gid"), col("sqlc"), col("sqlt"),col("rps"),col("rqt"));
		Date start21 =new Date();
		System.out.println("getPerflogs load data used time " + (start21.getTime() - start1.getTime()));
		datasetOrigin.printSchema();
		datasetOrigin.show();
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		
		Dataset<Row> datasetPerflogs = sparkSession.sql(
				"SELECT url, ucpu, mem, timestamp, gid,sqlc, sqlt, rps, rqt, urlNew  FROM monsoonperflogs monsoonperflogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid!=\"-\" and gid!=null order by timestamp desc ");
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSample.csv", datasetPerflogs);
		datasetPerflogs.show();
		
		Date start23 =new Date();
		System.out.println("getPerflogs filter data with sql time " + (start23.getTime() - start21.getTime()));	
		
		/*datasetOrigin = datasetOrigin.filter("timestamp > \""+ dateRangeArray[0] + "\" AND timestamp <\"" + dateRangeArray[1] + "\" and gid!=\"-\" and gid!=null ").orderBy(col("timestamp").asc());
		Date start22 =new Date();
		System.out.println("getPerflogs filter data used time " + (start22.getTime() - start21.getTime()));	*/	
		
		/*.filter(row -> {
					long timestampLong = row.getLong(1);
					System.out.println(timestampLong);
					Timestamp timeGot = new Timestamp(timestampLong);
					System.out.println(timeGot);
					return (timeGot.after(java.sql.Timestamp.valueOf(dateRangeArray[0]))) && (timeGot.before(java.sql.Timestamp.valueOf(dateRangeArray[1])));
		});*/
		Date start2 =new Date();
		datasetOrigin = datasetOrigin.withColumn("urlNew", callUDF("generalizeURLUDF", datasetOrigin.col("url")));
		

		datasetOrigin.show();
		Date start3 =new Date();
		System.out.println("getPerflogs change column data used time " + (start3.getTime() - start2.getTime()));
		/*Dataset<Row> datasetPerflogs = sparkSession.sql(
				"SELECT url, ucpu, mem, timestamp, gid,sqlc, sqlt, rps, rqt, urlNew  FROM monsoonperflogs monsoonperflogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid!=\"-\" and gid!=null order by timestamp desc ");
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSample.csv", datasetPerflogs);
		datasetPerflogs.show(); */

		return datasetOrigin;
	}

	public static Dataset<Row> getServerlogs(SparkSession sparkSession, String[] dateRangeArray) {
		Date start1 =new Date();
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonserverlogs")
				.select(col("gid"), col("timestamp"), col("params"), col("logContent"), col("host.name").as("apphost"))
				.filter("timestamp > \""+ dateRangeArray[0] + "\" AND timestamp <\"" + dateRangeArray[1] + "\"").orderBy(col("timestamp").asc());
		   /*.filter(row -> {
					long timestampLong = row.getLong(1);
					System.out.println(timestampLong);
					Timestamp timeGot = new Timestamp(timestampLong);
					System.out.println(timeGot);
					return (timeGot.after(java.sql.Timestamp.valueOf(dateRangeArray[0]))) && (timeGot.before(java.sql.Timestamp.valueOf(dateRangeArray[1])));
				});*/
		Date start2 =new Date();
		System.out.println("getServerlogs load data used time " + (start2.getTime() - start1.getTime()));
		datasetOrigin.createOrReplaceTempView("monsoonserverlogs");
		datasetOrigin.printSchema();
		
		/*Dataset<Row> dataServerlogs = sparkSession.sql(
				"SELECT  timestamp, gid, params, logContent FROM monsoonserverlogs monsoonserverlogs  where timestamp > \""
						+ dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1]
						+ "\" and gid!=\"-\" and gid!=null order by timestamp desc ");
		// CommonUtil.printTop5RowsOfDataset("monsoonServerlogsSample.csv",
		// dataServerlogs);
		dataServerlogs.show();
		Date start3 =new Date();
		System.out.println("getServerlogs select again data used time " + (start3.getTime() - start2.getTime())); */
		
		Date start3 =new Date();
		Dataset<Row> groupedByGidContents = datasetOrigin.groupBy(datasetOrigin.col("gid"))
				.agg(callUDF("groupContactLogs", datasetOrigin.col("logContent")).as("allLogContents"));
				//.orderBy(dataServerlogs.col("timestamp").asc());

		Date start4 =new Date();
		System.out.println("getServerlogs group  data used time " + (start4.getTime() - start3.getTime()));
		
		groupedByGidContents.show();

		return groupedByGidContents;
	}

	public static void joinPerfAndServerlogs(Dataset<Row> perfDataset, Dataset<Row> serverDataset) {
		// Dataset<Row> perfDataset : url, ucpu, mem, timestamp, gid,sqlc, sqlt, rps,
		// rqt, urlNew
		// Dataset<Row> serverDataset : gid, allLogContents
		Date start1 =new Date();
		Dataset<Row> joinedDataset = SparkProcessLogs.joinTwoDatasets(perfDataset, serverDataset, "inner", "gid",
				"gid");
		joinedDataset.show();
		Date start2 =new Date();
		System.out.println("joinPerfAndServerlogs join  data used time " + (start2.getTime() - start1.getTime()));
	}

	public static void word2Vec(Dataset<Row> documentDF, String savePath) throws Exception {
		/*
		 * List<Row> data = Arrays.asList(
		 * RowFactory.create(Arrays.asList(article1.split(" "))),
		 * RowFactory.create(Arrays.asList(article2.split(" "))) );
		 * 
		 * StructType schema = new StructType(new StructField[]{ new StructField("text",
		 * new ArrayType(DataTypes.StringType, true), false, Metadata.empty()) });
		 * Dataset<Row> documentDF = spark.createDataFrame(data, schema);
		 */

		// Learn a mapping from words to Vectors.
		Word2Vec word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(10).setMinCount(0);

		Word2VecModel model = word2Vec.fit(documentDF);
		model.save(savePath);

		Dataset<Row> result = model.transform(documentDF);

	}

	public static Vector combineTwoVectors(Vector vecA, Vector vecB) {

		double[] vecAArray = vecA.toArray();
		double[] vecBArray = vecB.toArray();

		double[] oneArray = ArrayUtils.addAll(vecAArray, vecBArray);

		DenseVector denseVec = new DenseVector(oneArray);

		return denseVec;
	}

	public static String readFileContents(String fileName) throws Exception {
		InputStream in = null;
		try {
			in = new FileInputStream(fileName);
			String fileContents;
			fileContents = IOUtils.toString(in);

			fileContents = fileContents.replace(",", " ");
			fileContents = fileContents.replace(".", " ");
			fileContents = fileContents.replace("_*_", " ");

			return fileContents;
		} catch (Exception e) {

		} finally {

		}
		return "";
	}

}
