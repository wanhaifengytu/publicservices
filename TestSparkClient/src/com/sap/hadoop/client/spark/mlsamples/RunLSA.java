package com.sap.hadoop.client.spark.mlsamples;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class RunLSA {
	protected static Logger logger = Logger.getLogger(RunLSA.class);

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";

		SparkSession session = SparkSession.builder().appName("RunLSA").master("local").getOrCreate();
		SparkContext sparkContext = session.sparkContext();

		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		int k = 100;
		int numTerms = 20000;

		AssembleDocumentTermMatrix assembleMatrix = new AssembleDocumentTermMatrix(session);

		testTFIDFSimple(session);
	}

	public static void testTFIDFSimple(SparkSession spark) {

		List<Row> data = Arrays.asList(RowFactory.create(0.0, "Hi I heard about Spark"),
				RowFactory.create(0.0, "I wish Java could use case classes"),
				RowFactory.create(1.0, "Logistic regression models are neat"));
		StructType schema = new StructType(
				new StructField[] { new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
						new StructField("sentence", DataTypes.StringType, false, Metadata.empty()) });
		Dataset<Row> sentenceData = spark.createDataFrame(data, schema);
		Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
		Dataset<Row> wordsData = tokenizer.transform(sentenceData);
		int numFeatures = 20;
		HashingTF hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
				.setNumFeatures(numFeatures);
		Dataset<Row> featurizedData = hashingTF.transform(wordsData);

		IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
		IDFModel idfModel = idf.fit(featurizedData);
		Dataset<Row> rescaledData = idfModel.transform(featurizedData);
		for (Row r : rescaledData.select("features", "label").takeAsList(3)) {
			Vector features = r.getAs(0);
			Double label = r.getDouble(1);
			System.out.println(features);
			System.out.println(label);
		}
	}

}
