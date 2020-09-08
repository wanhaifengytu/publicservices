package com.spark.analysis.elastic;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import com.spark.analysis.logs.CommonUtil;
import com.spark.analysis.logs.GeneralizeURLUDF;
import com.spark.analysis.logs.SparkProcessLogs;
import com.spark.analysis.logs.TimeStampToStringUDF;

public class TestElasticSparkAnalysis {
	
	protected static Logger logger = Logger.getLogger(TestElasticSparkAnalysis.class);
	
	public static void main(String[] args) throws Exception {
		
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);
		
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		
		String dateRange = (String)props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");
		
		int requestTimeThresh = Integer.parseInt((String)props.getProperty("nginxRequestTimeThresh"));
		
		Dataset<Row> nginxDs = testEsDatasetNginxlogs(sparkSession, dateRangeArray, requestTimeThresh);
		String gids = CommonUtil.concatGidString(nginxDs);
		
		testEsDatasetGetServerlogsFromGID(sparkSession, gids);
		testEsDatasetPerflogsFromGID(sparkSession, nginxDs, dateRangeArray, gids);
		
		//Test from perflog > 10s then get all server logs.
		testEsDatasetFromPerflogToServerlogs(sparkSession, requestTimeThresh, dateRangeArray);
		
		
		sparkSession.close();
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
	
	public static void testEsDatasetPerflogs(SparkSession sparkSession, Properties props) throws Exception {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs");
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");

		datasetOrigin.printSchema();
		
		String dateRange = (String)props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");
		Dataset<Row> datasetPerflogs = sparkSession.sql(
				"SELECT monsoonperflogs.CPU cpu, monsoonperflogs.MEM mem, monsoonperflogs.timestamp timestamp FROM monsoonperflogs monsoonperflogs  where timestamp > \"" 
		         + dateRangeArray[0] + "\" and timestamp <\"" + dateRangeArray[1] + "\" order by timestamp desc ");
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsSample.csv", datasetPerflogs);
		datasetPerflogs.show();

		// Write to Elastic
		// JavaEsSparkSQL.saveToEs(nginxDs, "newindexspark");
	}
	
	//23/Oct/2019:08:57:32 +0000
	public static Dataset<Row> testEsDatasetNginxlogs(SparkSession sparkSession, String[] dateRangeArray, int requestTimeThresh) {
		
		SparkContext sparkContext = sparkSession.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sparkContext);
		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.udf().register("generalizeURLUDF", new GeneralizeURLUDF(), DataTypes.StringType);
		sqlContext.udf().register("timeStampToStringUDF", new TimeStampToStringUDF(), DataTypes.StringType);
		
		Dataset<Row> datasetNginx = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonnginxaccesslogs");
		datasetNginx = datasetNginx.withColumn("timestampNew", to_timestamp(datasetNginx.col("timestamp"),"dd/MMM/yyyy:HH:mm:ss Z"));
		datasetNginx = datasetNginx.withColumn("timestampStr", callUDF("timeStampToStringUDF", datasetNginx.col("timestamp")));
		datasetNginx = datasetNginx.withColumn("requestURL",
				callUDF("generalizeURLUDF", datasetNginx.col("request")));
		datasetNginx.createOrReplaceTempView("monsoonnginxaccesslogs");
		
		Dataset<Row> nginxDs = sparkSession.sql(
				"SELECT  gid gidNginx, host.name nginxhost,timestampNew,timestampStr, request,  requestURL, requestLength, requestTime, bytes, response, message FROM monsoonnginxaccesslogs monsoonnginxaccesslogs where response= 200 " +
		          " and  requestTime > " + requestTimeThresh + " and timestampNew > \"" 
			       + dateRangeArray[0] + "\" and timestampNew <\""   + dateRangeArray[1] + "\" order by requestTime desc ");
		
		
		nginxDs.show(); 
		JavaEsSparkSQL.saveToEs(nginxDs, "monsoonnginxlogsselected");
		 
		return nginxDs;
	}
	
	public static void testEsDatasetGetServerlogsFromGID(SparkSession sparkSession, String gids) {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonserverlogs").select(col("gid"),col("timestamp"),col("params"),col("logContent"), col("host.name").as("apphost"));
		datasetOrigin.createOrReplaceTempView("monsoonserverlogs");

		datasetOrigin.printSchema();
		
		Dataset<Row> dataServerlogs = sparkSession.sql(
				"SELECT apphost, timestamp, gid, params, logContent FROM monsoonserverlogs monsoonserverlogs  where gid in (" 
			    + gids + ") order by timestamp desc ");
		
		//CommonUtil.printTop5RowsOfDataset("monsoonServerlogsSample.csv", dataServerlogs);
		dataServerlogs.show();
		
		/*Dataset <Row> joinedTop5ServerData = SparkProcessLogs.joinTwoDatasets(dataServerlogs, nginxDs, "inner", "gid", "gidNginx");
		joinedTop5ServerData.printSchema();
		joinedTop5ServerData.show();
		 */
		
		JavaEsSparkSQL.saveToEs(dataServerlogs, "monsoonserverlogsselected");
		System.out.println("save to Es of joinedTop5PerfData");
	}
	
	public static void testEsDatasetPerflogsFromGID(SparkSession sparkSession,  Dataset<Row> nginxDs,  String[] dateRangeArray, String gids) throws Exception {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs").select(col("timestamp"), col("gid"), col("url"), col("host.name").as("apphost"), col("rqt"), col("scpu"), col("ucpu"), col("mem"), col("cpu"), col("sqlc"), col("sqlt"));
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");

		//datasetOrigin.printSchema();
		/*String gidTop = (String)((Row)top5LongReqs.get(0)).get(6); */
		Dataset<Row> datasetPerflogs = sparkSession.sql(
				"SELECT gid, cpu, mem, timestamp, apphost, rqt,scpu, ucpu, sqlc, sqlt,url  FROM monsoonperflogs monsoonperflogs  where   gid in (" 
						 + gids + ") order by timestamp desc ");
 
		datasetPerflogs.show(); 

		/*Dataset <Row> joinedTop5PerfData = SparkProcessLogs.joinTwoDatasets(datasetPerflogs, nginxDs, "inner", "gid", "gidNginx");
		joinedTop5PerfData.printSchema();
		joinedTop5PerfData.show(); */
		
		JavaEsSparkSQL.saveToEs(datasetPerflogs, "monsoonperflogsselected");
		System.out.println("save to Es of joinedTop5PerfData");

	}
	
	public static void testEsDatasetFromPerflogToServerlogs(SparkSession sparkSession, int requestTimeThresh,  String[] dateRangeArray) {
		Dataset<Row> datasetOrigin = sparkSession.read().format("org.elasticsearch.spark.sql").load("monsoonperflogs").select(col("timestamp"), col("gid"), col("url"), col("host.name").as("apphost"), col("rqt"), col("scpu"), col("ucpu"), col("mem"), col("cpu"), col("sqlc"), col("sqlt"));
		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		
		Dataset<Row> perflogDs = sparkSession.sql(
				"SELECT  gid ,timestamp, url, apphost, rqt, scpu,ucpu, mem,cpu, sqlc, sqlt FROM monsoonperflogs monsoonperflogs where " +
		          "  rqt > " + requestTimeThresh + " and timestamp > \"" 
			       + dateRangeArray[0] + "\" and timestamp <\""   + dateRangeArray[1] + "\" order by rqt desc ");
		
		JavaEsSparkSQL.saveToEs(perflogDs, "monsoonperflogsgreater10s");
		
		String gids = CommonUtil.concatGidString(perflogDs);
		
		Dataset<Row> dataServerlogs = sparkSession.sql(
				"SELECT apphost, timestamp, gid, params, logContent FROM monsoonserverlogs monsoonserverlogs  where gid in (" 
			    + gids + ") order by timestamp desc ");
		
		JavaEsSparkSQL.saveToEs(dataServerlogs, "monsoonserverlogsgreater10srelated");
	}
}
