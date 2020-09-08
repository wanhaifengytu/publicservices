package com.spark.simple.sample;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.spark.analysis.logs.CommonUtil;

public class TestTFIDF {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of spark program");
		Properties props =  CommonUtil.loadProperties("sparkParams.properties");
		
		if(props.getProperty("hadoop.home.dir")!=null) {
			System.setProperty("hadoop.home.dir", (String)props.getProperty("hadoop.home.dir"));
		}
		
		String master = (String)props.getProperty("master");
		
		SparkSession session = SparkSession.builder().appName("TestTFIDF").master(master).getOrCreate();
		SparkContext sparkContext = session.sparkContext();
		JavaSparkContext jsc = new JavaSparkContext(sparkContext);
		
		 List<Row> data = Arrays.asList(
	                RowFactory.create(0.0, "Welcome to TutorialKart."),
	                RowFactory.create(0.0, "Learn Spark at TutorialKart."),
	                RowFactory.create(1.0, "Spark Mllib has TF-IDF.")
	                );
	        StructType schema = new StructType(new StructField[]{
	                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
	                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
	        });
	        
	      // import data with the schema
	      Dataset<Row> sentenceData = session.createDataFrame(data, schema);
	      
	      // break sentence to words
	      Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
	      Dataset<Row> wordsData = tokenizer.transform(sentenceData);
	        
	        
	      int numTerms = 2000;
	      CountVectorizer countVectorizer = new CountVectorizer().setInputCol("words").setOutputCol("termFreqs").setVocabSize(numTerms);
	      CountVectorizerModel countVectorizerModel = countVectorizer.fit(wordsData);
	      Dataset<Row> docTermFreqs = countVectorizerModel.transform(wordsData);
	      
	      CommonUtil.printAllRowsOfDataset("./docTermFreqs.csv", docTermFreqs);
	      docTermFreqs.cache();
	      
	      IDF idf = new IDF().setInputCol("termFreqs").setOutputCol("tdidfVec");
	      IDFModel idfModel = idf.fit(docTermFreqs);
	      Dataset<Row> docTermMatrix = idfModel.transform(docTermFreqs);
	      CommonUtil.printAllRowsOfDataset("./docTermMatrix.csv", docTermMatrix);
	      
	}

}
