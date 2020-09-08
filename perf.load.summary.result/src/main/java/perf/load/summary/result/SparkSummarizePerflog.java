package perf.load.summary.result;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.min;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.sum;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class SparkSummarizePerflog {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of main program");
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		SQLContext sqlContext = new SQLContext(jsc);
		
		Map options = new HashMap();
		options.put("pushdown", "true");
		options.put("es.nodes", (String) props.getProperty("esIPs"));
		options.put("es.port", (String) props.getProperty("esPort"));
		
		
		RestHighLevelClient esClient = getClient(props);
		//removeElasticIndex("monsoonperflogbase", esClient);
		//removeElasticIndex("monsoonperflognew", esClient);
		
		
	/*	String dateRange = (String) props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");
		Dataset<Row> perflogsBaseDataset = getPerflogsBySQL(sqlContext, options,  dateRangeArray);
		summarizePerflog(perflogsBaseDataset, "monsoonperflogbase");

		String dateRangeNew = (String) props.getProperty("comparedTimeRange");
		String[] dateRangeNewArray = dateRangeNew.split(",");
		Dataset<Row> perflogsNewDataset = getPerflogsBySQL(sqlContext, options,  dateRangeNewArray);
		summarizePerflog(perflogsBaseDataset, "monsoonperflognew");		
		*/
		
		
		Dataset<Row> summaryPerflogBase = getSummarizedLogDataset(sqlContext, options, "monsoonperflogbase");
		Dataset<Row> summaryPerflogNew = getSummarizedLogDataset(sqlContext, options, "monsoonperflognew");
		
		comparePerflogSummaryStepLevel(summaryPerflogBase, summaryPerflogNew, "basenewcomparison");
		
		
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
				.load("monsoonperflogs").select(col("testiter"), col("testreqname"), col("testscriptname"), col("teststep"), col("host.name").as("hostName"),
						col("URL"), col("UN"), col("RPS"), col("RQT"), col("GID"), col("EID"), col("CMID"), col("logdate"));

		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		Date start21 = new Date();
		System.out.println("perf:::::::::::getPerflogs load data used time " + (start21.getTime() - start1.getTime()));

		Date start22 = new Date();
		Dataset<Row> datasetPerflogs = sqlContext.sql("SELECT testiter, testreqname, testscriptname, teststep, hostName, URL, UN, RPS, RQT,GID,EID,CMID,logdate "
				        + " FROM monsoonperflogs monsoonperflogs " 
				        + " where logdate > \"" + dateRangeArray[0] + "\" and logdate <\"" + dateRangeArray[1]
						+ "\" and GID IS NOT NULL "
						+ " order by logdate asc");
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsFilteredByTime.csv", datasetPerflogs);


		Date start23 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs filter data with sql time " + (start23.getTime() - start22.getTime()));
		
		return datasetPerflogs;
	}
	
	
	public static void summarizePerflog(Dataset<Row> datasetPerflogs, String indexName) {
		Dataset<Row> groupedData = datasetPerflogs.groupBy("testscriptname", "teststep")
				.agg(avg(datasetPerflogs.col("RQT")).as("avgRQT"),stddev(datasetPerflogs.col("RQT")).as("stddevRQT"),
					 min(datasetPerflogs.col("testiter")).as("minIter"), max(datasetPerflogs.col("testiter")).as("maxIter"),
					 sum(datasetPerflogs.col("RQT")).as("sumRQT"), count(datasetPerflogs.col("teststep")).as("countSteps"))
				.orderBy("testscriptname", "teststep");
		
		groupedData.show();
		CommonUtil.printTop5RowsOfDataset("perflogGroupByTestScriptStep.csv", groupedData);
		
		JavaEsSparkSQL.saveToEs(groupedData, indexName);
	}
	
	public static Dataset<Row> getSummarizedLogDataset(SQLContext sqlContext, Map options, String indexName) {
		Dataset<Row> datasetOrigin = sqlContext.read().format("org.elasticsearch.spark.sql").options(options)
				.load(indexName).select(col("testscriptname"), col("teststep"), col("avgRQT"), col("maxIter"), col("minIter"));
		
		datasetOrigin.createOrReplaceTempView(indexName);
		
		return datasetOrigin;
	}
	
	
	public static void comparePerflogSummaryStepLevel(Dataset<Row> perflogSummaryBase, Dataset<Row> perflogSummaryNew, String indexName) {
		perflogSummaryBase = perflogSummaryBase.withColumn("testscriptnameBase", col("testscriptname")).drop(col("testscriptname"))
				                               .withColumn("teststepBase", col("teststep")).drop(col("teststep"))
				                               .withColumn("minIterBase", col("minIter")).drop(col("minIter"))
				                               .withColumn("maxIterBase", col("maxIter")).drop(col("maxIter"))
				                               .withColumn("avgRQTBase", col("avgRQT")).drop(col("avgRQT"));
		
		Dataset<Row> joinedDataSet = perflogSummaryBase.join(perflogSummaryNew, 
				col("testscriptnameBase").equalTo(col("testscriptname")).and(col("teststepBase").equalTo(col("teststep"))), "left_outer")
				.withColumn("stepDiff", col("avgRQT").minus(col("avgRQTBase")).divide(col("avgRQTBase").plus(1)));	
		
		
		joinedDataSet.show();
		JavaEsSparkSQL.saveToEs(joinedDataSet, indexName);
	}
	
	/**
	 * Highlevel ESClient
	 * 
	 * @return
	 */
	public static RestHighLevelClient getClient(Properties props) {
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost((String) props.getProperty("esIPs"), (Integer.parseInt(props.getProperty("esPort"))), "http")));
		return client;
	}
	
	public static void removeElasticIndex(String indexName, RestHighLevelClient client) {
		try {
		    DeleteIndexRequest request = new DeleteIndexRequest(indexName);
		    client.indices().delete(request, RequestOptions.DEFAULT);
		} catch (ElasticsearchException exception) {
		    if (exception.status() == RestStatus.NOT_FOUND) {
		        System.err.print("The index " + indexName + " doesnt exist..");
		    }
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
