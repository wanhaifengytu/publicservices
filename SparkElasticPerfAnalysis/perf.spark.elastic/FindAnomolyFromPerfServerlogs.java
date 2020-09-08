package perf.spark.elastic.perf.spark.elastic;


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


public class FindAnomolyFromPerfServerlogs {

	public static void main(String[] args)throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

		SQLContext sqlContext = new SQLContext(jsc);
		sqlContext.udf().register("generalizeURLUDF", new GeneralizeURLUDFPerflog(), DataTypes.StringType);
		sqlContext.udf().register("groupContactLogs", new GroupConcatColumnUDAF());

		String dateRange = (String) props.getProperty("selectedTimeRange");
		String[] dateRangeArray = dateRange.split(",");
		
		Dataset<Row> perflogDataset = getPerflogs(sparkSession, dateRangeArray);
		
		
		
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
		
		Date start2 =new Date();
		datasetOrigin = datasetOrigin.withColumn("urlNew", callUDF("generalizeURLUDF", datasetOrigin.col("url")));

		datasetOrigin.show();
		Date start3 =new Date();
		System.out.println("getPerflogs change column data used time " + (start3.getTime() - start2.getTime()));

		return datasetOrigin;
	}
}
