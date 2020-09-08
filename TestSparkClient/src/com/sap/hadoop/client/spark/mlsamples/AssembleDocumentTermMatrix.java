package com.sap.hadoop.client.spark.mlsamples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class AssembleDocumentTermMatrix {
	private SparkSession session;

	public AssembleDocumentTermMatrix(SparkSession session) {
		super();
		this.session = session;
	}
	
	public Dataset<String> parseWikipediaDump() {
		
		
		return null;
	}
}
