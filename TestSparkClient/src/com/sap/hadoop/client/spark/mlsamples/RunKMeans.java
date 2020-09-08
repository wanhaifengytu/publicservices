package com.sap.hadoop.client.spark.mlsamples;

import java.util.Arrays;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.sap.hadoop.client.spark.CommonUtil;
import com.sap.hadoop.client.spark.perfload.logs.TestMlLibSample;

public class RunKMeans {

	protected static Logger logger = Logger.getLogger(TestMlLibSample.class);

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";

		SparkSession session = SparkSession.builder().appName("RunKMeans").master("local").getOrCreate();
		SparkContext sparkContext = session.sparkContext();

		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
		Dataset<Row> dataset = loadCSVFile(session,
				"D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\data\\kddcup.data_10_percent.csv");

		clusteringScore0(dataset, 3);
		fitPipeline4(dataset, 4);
	}

	public static Dataset<Row> loadCSVFile(SparkSession session, String csvFilePath) {

		Dataset<Row> dataset = session.read().option("format", "com.databricks.csv").option("header", "false")
				.option("inferSchema", "false").csv(csvFilePath).toDF("duration", "protocol_type", "service", "flag",
						"src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent", "hot", "num_failed_logins",
						"logged_in", "num_compromised", "root_shell", "su_attempted", "num_root", "num_file_creations",
						"num_shells", "num_access_files", "num_outbound_cmds", "is_host_login", "is_guest_login",
						"count", "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
						"same_srv_rate", "diff_srv_rate", "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
						"dst_host_same_srv_rate", "dst_host_diff_srv_rate", "dst_host_same_src_port_rate",
						"dst_host_srv_diff_host_rate", "dst_host_serror_rate", "dst_host_srv_serror_rate",
						"dst_host_rerror_rate", "dst_host_srv_rerror_rate", "label");

		dataset.cache();

		dataset.select("label").groupBy("label").count().show(25);

		Dataset<Row> numericOnly = dataset.drop("protocol_type", "service", "flag").cache();
		for (String c : numericOnly.columns()) {
			numericOnly = numericOnly.withColumn(c, numericOnly.col(c).cast("double"));
		}
		return numericOnly;
	}


	public static void clusteringScore0(Dataset dataset, int k) {

		dataset = dataset.drop("label");

		dataset.show();
		dataset.printSchema();

		String[] inputCols = CommonUtil.filterStringArray(dataset.columns(), "label");

		VectorAssembler assembler = new VectorAssembler().setInputCols(dataset.columns()).setOutputCol("featureVector")
				.setHandleInvalid("skip");

		long rndLong = (long) Math.random() * 99999;
		KMeans kmeans = new KMeans().setSeed(rndLong).setPredictionCol("cluster").setFeaturesCol("featureVector");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { assembler, kmeans });
		PipelineModel pipelineModel = pipeline.fit(dataset);

		Transformer[] allTransformers = pipelineModel.stages();
		Transformer lastTransformer = allTransformers[allTransformers.length - 1];
		KMeansModel kmeansModel = (KMeansModel) lastTransformer;

		Dataset<Row> transformedData = assembler.transform(dataset);
		System.out.println("Transformer Data");
		transformedData.show();

		double costCal = kmeansModel.computeCost(transformedData) / dataset.count();

		System.out.println("cost " + costCal);
	}

	public static void fitPipeline4(Dataset dataset, int k) {
		dataset = dataset.drop("label");
		VectorAssembler assembler = new VectorAssembler().setInputCols(dataset.columns()).setOutputCol("featureVector").setHandleInvalid("skip");

		StandardScaler scaler = new StandardScaler().setInputCol("featureVector").setOutputCol("scaledFeatureVector")
				.setWithStd(true).setWithMean(false);
		
		long rndLong = (long) Math.random() * 99999;
		KMeans kmeans = new KMeans().setSeed(rndLong).setPredictionCol("cluster").setFeaturesCol("scaledFeatureVector").setMaxIter(40);
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { assembler, scaler, kmeans });
		PipelineModel pipelineModel = pipeline.fit(dataset);
		
		Transformer[] allTransformers = pipelineModel.stages();
		Transformer lastTransformer = allTransformers[allTransformers.length - 1];
		KMeansModel kmeansModel = (KMeansModel) lastTransformer;

		Dataset<Row> transformedData = assembler.transform(dataset);
		System.out.println("fitPipeline4 Transformer Data");
		transformedData.show();

		Dataset<Row> transDataset = pipelineModel.transform(dataset);
		transDataset.show();
		double costCal = kmeansModel.computeCost(transDataset) ;

		System.out.println("cost " + costCal);
		System.out.println("cost ratio " + costCal/ dataset.count());
	}

	public static Pipeline oneHotPipeline(String inputCol) {
		StringIndexer indexer = new StringIndexer().setInputCol(inputCol).setOutputCol(inputCol + "_indexed");
		OneHotEncoder encoder = new OneHotEncoder().setInputCol(inputCol + "_indexed").setOutputCol(inputCol + "_vec");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { indexer, encoder });

		return pipeline;
	}
}
