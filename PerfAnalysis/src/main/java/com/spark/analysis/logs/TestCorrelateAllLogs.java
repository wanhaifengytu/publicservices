package com.spark.analysis.logs;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.stddev_pop;
import static org.apache.spark.sql.functions.window;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;


public class TestCorrelateAllLogs {

	/**
	 * We use a logger to print the output. Sl4j is a common library which works
	 * with log4j, the logging system used by Apache Spark.
	 */
	// private static final Logger LOGGER =
	// LoggerFactory.getLogger(PerfLogParseTask.class);
	protected static Logger logger = Logger.getLogger(TestCorrelateAllLogs.class);

	/**
	 * This is the entry point when the task is called from command line with
	 * spark-submit.sh. See
	 * {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of test Spark Program");
		Properties props =  CommonUtil.loadProperties("sparkParams.properties");
		
		if(props.getProperty("hadoop.home.dir")!=null) {
			System.setProperty("hadoop.home.dir", (String)props.getProperty("hadoop.home.dir"));
		}
		
		String master = (String)props.getProperty("master");
		
		SparkSession session = SparkSession.builder().appName("TestCorrelateAllLogs").master(master).getOrCreate();
		SparkContext sparkContext = session.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sparkContext);
		
		SQLContext sqlContext = new SQLContext(jsc);
		
		Dataset<Row> perfLogRows = SparkProcessLogs.parseMultipleFiles(session,(String)props.getProperty("perflogCSVPath"), "perflog" );
		perfLogRows.show();
		
		Dataset<Row> serverLogRows = SparkProcessLogs.parseMultipleFiles(session,(String)props.getProperty("serverlogCSVPath"), "serverlog" );
		serverLogRows.show();
		
		Dataset<Row> nginxLogRows = SparkProcessLogs.parseMultipleFiles(session,(String)props.getProperty("nginxlogCSVPath"), "nginxlog" );
		nginxLogRows.show();
		
		new TestCorrelateAllLogs().analysisNginxLogsWithPerfServerLogs(session, nginxLogRows,serverLogRows, perfLogRows, sqlContext);
		
		System.out.println("Done analysis all");
	}	
	
	public void analysisNginxLogsWithPerfServerLogs(SparkSession session, Dataset<Row> detailedRow, Dataset<Row> serverLogsRow, Dataset<Row> perfLogsRow, SQLContext sqlContext) {
		// gm = new GeometricMean
		GroupConcatDistinctUDAF groupContactDistinct = new GroupConcatDistinctUDAF();
		
		sqlContext.udf().register("groupContactDistinct", groupContactDistinct);
		sqlContext.udf().register("generalizeURLUDF", new GeneralizeURLUDF(), DataTypes.StringType);

		//Apply UDF to get a new column
		detailedRow = detailedRow.withColumn("requestURLNew",
				callUDF("generalizeURLUDF", detailedRow.col("requestURL")));
		// ##Key point: We need to run a count operation to make sure it is Running.
		System.out.println("count of detailed row transform " + detailedRow.count());
		CommonUtil.printAllRowsOfDataset("./nginxDetailedInitial.csv", detailedRow);

		detailedRow.createOrReplaceTempView("NginxlogTempView");
		detailedRow.printSchema();
		
		//1. Every time period(1 minute, 5 minutes, 10 minutes), all requests: request Count/req time(avg, 90 percentile)/reqLengh(avg, 90 percentile)/bytesSent(avg, 90 percentile)
		//df.groupBy(window($"time", "1 minute", "10 seconds"))
		Dataset<Row> grpByTimeIntervalThenAgg = detailedRow.groupBy(window(detailedRow.col("timeLocal"), "5 minutes")).agg(count(detailedRow.col("GID")), 
				   avg(detailedRow.col("requestTime")), avg(detailedRow.col("requestLength")),avg(detailedRow.col("bytesSent")), callUDF("groupContactDistinct", detailedRow.col("requestURLNew")).as("concatURL")).orderBy("window");
		
		CommonUtil.printAllRowsOfDataset("./grpByTimeIntervalNginx.csv", grpByTimeIntervalThenAgg);
		
		//2.Group on type of request, for each type, find the abnormal request.
		Dataset<Row> sortedNginxLogRows = session.sql("select timeLocal, requestURLNew, companyId, requestLength, bytesSent,requestTime from NginxlogTempView order by requestURLNew, companyId, timeLocal asc");
		CommonUtil.printAllRowsOfDataset("./sortedRowsNginxLog.csv", sortedNginxLogRows);
		sortedNginxLogRows.printSchema();
		
		Dataset<Row> grpByRequestURL = sortedNginxLogRows.groupBy(sortedNginxLogRows.col("requestURLNew")).agg(avg(sortedNginxLogRows.col("requestTime")).as("avg_reqTime"), 
				stddev_pop(sortedNginxLogRows.col("requestTime")).as("stdDev_reqTime"));
		
		grpByRequestURL = grpByRequestURL.withColumn("reqTimeStdDevFactor", col("stdDev_reqTime").divide(col("avg_reqTime"))).orderBy(col("reqTimeStdDevFactor").desc());
		grpByRequestURL.printSchema();
		CommonUtil.printAllRowsOfDataset("./grpByRequestURLNginx.csv", grpByRequestURL);
		
		//3. Find the top 5 standard deviation factors page, find corresponding max respTime request
		List<Row> top5Rows = grpByRequestURL.takeAsList(5);
		Map hashMap = new HashMap();
		for (Row row : top5Rows) {
			String requestURLNew = (String)row.get(0);
			hashMap.put(requestURLNew, 1);
		}
		detailedRow.printSchema();
		Dataset<Row> filteredRespTime = detailedRow.filter(row -> {
			if (hashMap.containsKey(row.get(14))) {
				return true;
			} else {
				return false;
			}
		});
		filteredRespTime.count();
		//CommonUtil.printTop5RowsOfDataset("./Top5filteredTopRespTime", filteredTopRespTime);
		CommonUtil.printAllRowsOfDataset("./filteredTopRespTime.csv", filteredRespTime);
		
		//4.Use window function to get top5 of each group 
		//https://stackoverflow.com/questions/38397796/retrieve-top-n-in-each-group-of-a-dataframe-in-pyspark
		//http://timepasstechies.com/window-functions-in-spark-sql-and-dataframe-ranking-functionsanalytic-functions-and-aggregate-function/
		WindowSpec windowDesc = Window.partitionBy("requestURLNew").orderBy(filteredRespTime.col("requestTime").desc());
		WindowSpec windowAsc = Window.partitionBy("requestURLNew").orderBy(filteredRespTime.col("requestTime").asc());
		Column  columnRankDesc = rank().over(windowDesc);
		Column  columnRankAsc = rank().over(windowAsc);
		
		Dataset<Row> rankedURL = filteredRespTime.select(filteredRespTime.col("*"), columnRankDesc.alias("rankDesc"), columnRankAsc.alias("rankAsc"));
		rankedURL.printSchema();
		CommonUtil.printAllRowsOfDataset("./rankedURLOrigin.csv", rankedURL);
		
		Dataset<Row> top5EachURL = rankedURL.where(rankedURL.col("rankDesc").leq(5));
		CommonUtil.printAllRowsOfDataset("./top5EachURL.csv", top5EachURL);
		
		Dataset<Row> bottom5EachURL = rankedURL.where(rankedURL.col("rankAsc").leq(5));
		CommonUtil.printAllRowsOfDataset("./bottomEachURL.csv", bottom5EachURL);	
		
		//5. Join top 5 url with server log to get details.
		//From GID to correlate to corresponding perf.log items and server.log items. Find the similarity of lines
	    //https://stackoverflow.com/questions/43033835/join-in-a-dataframe-spark-java
		Dataset <Row> joinedTop5ServerData = SparkProcessLogs.joinTwoDatasets(top5EachURL, serverLogsRow, "inner", "GID", "GID");
		joinedTop5ServerData.printSchema();
		CommonUtil.printAllRowsOfDataset("./joinedTop5ServerData.csv", joinedTop5ServerData);
		
		//6. Join bottom 5 url with server log to get details
		Dataset <Row> joinedBottom5ServerData = SparkProcessLogs.joinTwoDatasets(bottom5EachURL, serverLogsRow, "inner", "GID", "GID");
		joinedBottom5ServerData.printSchema();
		CommonUtil.printAllRowsOfDataset("./joinedBottom5ServerData.csv", joinedBottom5ServerData);
		
		//Join top5 perf log
		Dataset <Row> joinedTop5PerfData = SparkProcessLogs.joinTwoDatasets(top5EachURL, perfLogsRow, "inner", "GID", "GID");
		joinedTop5PerfData.printSchema();
		CommonUtil.printAllRowsOfDataset("./joinedTop5PerfData.csv", joinedTop5PerfData);
		
		//Join bottom5 perf log
		Dataset <Row> joinedBottom5PerfData = SparkProcessLogs.joinTwoDatasets(bottom5EachURL, perfLogsRow, "inner", "GID", "GID");
		joinedBottom5PerfData.printSchema();
		CommonUtil.printAllRowsOfDataset("./joinedBottom5PerfData.csv", joinedBottom5PerfData);
	}
	
	public Dataset<Row> analyzePerflogs(SparkSession session, String perflogCSVPath) {
		Dataset<Row> detailedRow = SparkProcessLogs.parsePerflog(session,perflogCSVPath);
		detailedRow.show();
		
		detailedRow.createOrReplaceTempView("PerflogTempView");
		
		Dataset<Row> sortedRows = session.sql("select * from PerflogTempView order by CMID, URL, UID, dateTime desc");
		CommonUtil.printAllRowsOfDataset("./sortedRowsPerflog.csv", sortedRows);
		
		
		return detailedRow;
	}
	
	public Dataset<Row> analyzeServerLogs(SparkSession session, String serverLogCSVPath) {

		Dataset<Row> rowServerLogs = SparkProcessLogs.parseServerLogs(session,serverLogCSVPath);
		
		rowServerLogs.createOrReplaceTempView("detailedServerlog");

		Dataset<Row> groupedByFuncServerLogs = session.sql(
				"select function as function, count(count) as countCount from detailedServerlog group by function");
		CommonUtil.printAllRowsOfDataset("./groupedByFuncServerLog.csv", groupedByFuncServerLogs);
		
		return rowServerLogs;
	}
	
	/**
	 * The task body
	 */
	public Dataset<Row> processPerfLogs(SparkSession session, SparkContext sparkContext, String perflogPath) {
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		Dataset<Row> detailedRow = SparkProcessLogs.parsePerflog(session, perflogPath);

		detailedRow.createOrReplaceTempView("detailedPerflog");

		Dataset<Row> selectedRows = session.sql("select * from detailedPerflog where SQLT>100 order by SQLT desc");
		CommonUtil.printTop5RowsOfDataset("./selectedRows.csv", selectedRows);

		Dataset<Row> groupedURLRows = session.sql(
				"select URL, count(CMN) as countCompany, avg(RQT) as avrReq from detailedPerflog group by URL order by avrReq desc");
		CommonUtil.printTop5RowsOfDataset("./groupByURL.csv", groupedURLRows);

		return detailedRow;
	}

	/**
	 * # log setting log_format mycombined '$remote_addr $proxy_add_x_forwarded_for
	 * $remote_user - [$time_local] ' '"$request" $status $bytes_sent
	 * "$http_referer" "$http_user_agent" ' '"$custom_JSESSIONID" "$request_id"
	 * "$cookie_bizxCompanyID" $request_time $request_length';
	 * 
	 * @param session
	 * @param sparkContext
	 */
	public void processNginxLogs(SparkSession session, SparkContext sparkContext, String nginxLogPath) {
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		Dataset<Row> detailedRow = SparkProcessLogs.parseNginxLogs(session,nginxLogPath);

		// ##Key point: We need to run a count operation to make sure it is Running.
		System.out.println("count of detailed row transform " + detailedRow.count());
		CommonUtil.printTop5RowsOfDataset("./nginxDetailed.csv", detailedRow);

		detailedRow.createOrReplaceTempView("detailedNginxlog");

		Dataset<Row> selectedRows = session.sql("select companyId, requestTime from detailedNginxlog");
		CommonUtil.printAllRowsOfDataset("./selectedNginxRows.csv", selectedRows);

		Dataset<Row> groupedURLRows = session.sql(
				"select jSessionId as sessionId, sum(requestTime) as totalRegTime from detailedNginxlog group by jSessionId");
		CommonUtil.printAllRowsOfDataset("./groupBySessionNginxLogs.csv", groupedURLRows);

	}

	public void processServerLogs(SparkSession session, SparkContext sparkContext, String serverLogpath) {

		Dataset<Row> rowServerLogs = SparkProcessLogs.parseServerLogs(session,serverLogpath);
		
		rowServerLogs.createOrReplaceTempView("detailedServerlog");

		Dataset<Row> groupedByFuncServerLogs = session.sql(
				"select function as function, count(count) as countCount from detailedServerlog group by function");
		CommonUtil.printAllRowsOfDataset("./groupedByFuncServerLogs.csv", groupedByFuncServerLogs);
		
	}

}
