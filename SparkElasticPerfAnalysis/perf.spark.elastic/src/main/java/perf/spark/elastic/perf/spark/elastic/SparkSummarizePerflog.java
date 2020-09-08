package perf.spark.elastic.perf.spark.elastic;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

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
		
		String dateRange = (String) props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");
		Dataset<Row> perflogsDataset = getPerflogsBySQL(sqlContext, options,  dateRangeArray);
		
		

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
				.load("monsoonperflogs").select(col("testiter"), col("testreqname"), col("testscriptname"), col("teststep"), col("host.name"),
						col("URL"), col("UN"), col("RPS"), col("RQT"), col("GID"), col("EID"), col("CMID"), col("logdate"));

		datasetOrigin.createOrReplaceTempView("monsoonperflogs");
		Date start21 = new Date();
		System.out.println("perf:::::::::::getPerflogs load data used time " + (start21.getTime() - start1.getTime()));

		Date start22 = new Date();  //host.name
		Dataset<Row> datasetPerflogs = sqlContext.sql(
				"SELECT testiter, testreqname, testscriptname, teststep, host.name, URL, UN, RPS, RQT,GID,EID,CMID FROM monsoonperflogs monsoonperflogs " 
				        + " where logdate > \"" + dateRangeArray[0] + "\" and logdate <\"" + dateRangeArray[1]
						+ "\" and GID IS NOT NULL "
						+ " order by logdate asc");
		CommonUtil.printTop5RowsOfDataset("monsoonperflogsFilteredByTime.csv", datasetPerflogs);


		Date start23 = new Date();
		System.out.println(
				"perf:::::::::::getPerflogs filter data with sql time " + (start23.getTime() - start22.getTime()));

		return datasetPerflogs;
	}
	
}
