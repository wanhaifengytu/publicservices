package com.sap.hadoop.client.spark.perfload.logs;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.sap.hadoop.client.spark.CommonUtil;

public class TestMlLibSample {

	protected static Logger logger = Logger.getLogger(TestMlLibSample.class);

	/**
	 * This is the entry point when the task is called from command line with
	 * spark-submit.sh. See
	 * {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";

		SparkSession session = SparkSession.builder().appName("TestMlLibSample").master("local").getOrCreate();
		SparkContext sparkContext = session.sparkContext();

		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		//CommonUtil.changeCSVtoLibSVM(session,
		//		"D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\vmload_cfNoTime.csv", "free");


		//handleInsuranceExample(session,
			//	"D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\FL_insurance_sample.csv");
		handleLibSVMFeatures(session, "D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\vmload.libsvm");
	}

	public static void handleInsuranceExample(SparkSession session, String filePath) {
		Dataset<Row> dataset = session.read().option("format", "com.databricks.csv").option("header", "true")
				.option("inferSchema", "true").csv(filePath);

		dataset.show();

		// One hot encoder.
		StringIndexer indexercounty = new StringIndexer().setInputCol("county").setOutputCol("countryIndex");
		StringIndexer indexerconstruction = new StringIndexer().setInputCol("construction")
				.setOutputCol("constructionIndex");
		StringIndexer indexerstatecode = new StringIndexer().setInputCol("statecode").setOutputCol("statecodeIndex");

		StringIndexer indexerline = new StringIndexer().setInputCol("line").setOutputCol("lineIndex");

		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { indexercounty, indexerconstruction, indexerstatecode, indexerline });
		Dataset<Row> datasetEncoded = pipeline.fit(dataset).transform(dataset).drop("county").drop("construction")
				.drop("statecode").drop("line");

		datasetEncoded.show();

		CommonUtil.saveDatasetToCSV(datasetEncoded, filePath + "encode.csv");

	}

	public static void handleLibSVMFeatures(SparkSession session, String libSVMPath) {
		Dataset<Row> data = session.read().format("libsvm").load(libSVMPath);

		Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("hu_site_limit_norm");
		StandardScaler scaler = new StandardScaler().setWithMean(true).setWithStd(true).setInputCol("features")
				.setOutputCol("scaledFeatures");

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] { scaler, normalizer });

        // Fit the pipeline on training data.
        System.out.println("\nTraining...");
        PipelineModel model = pipeline.fit(data);
        
        Dataset<Row> scaledDataset = model.transform(data);
        scaledDataset.show();
        CommonUtil.printAllRowsOfDataset("normscaleddata.csv", scaledDataset);
	}

	public static void createData(SparkSession session) {
		List<Row> data = Arrays.asList(
				RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 1.0, -2.0 })),
				RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
				RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
				RowFactory.create(Vectors.sparse(4, new int[] { 0, 3 }, new double[] { 9.0, 1.0 })));

		StructType schema = new StructType(
				new StructField[] { new StructField("features", new VectorUDT(), false, Metadata.empty()), });

		Dataset<Row> df = session.createDataFrame(data, schema);
		df.show();
	}

}
