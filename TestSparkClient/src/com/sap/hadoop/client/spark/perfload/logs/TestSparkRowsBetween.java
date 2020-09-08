package com.sap.hadoop.client.spark.perfload.logs;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.lag;
import static org.apache.spark.sql.functions.last;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;


import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

public class TestSparkRowsBetween {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";
		
		SparkSession session = SparkSession.builder().appName("TestSparkRowsBetween").master(master).getOrCreate();
		
		
		new TestSparkRowsBetween().testRowsBetween(session, "D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\hw_200.csv");
	}
	
	public void testRowsBetween(SparkSession session, String csvPath) {
		Dataset<Row> vmloadLogDataset = session.read().option("format","com.databricks.csv").option("header","true").option("inferSchema", "true")
				.csv(csvPath);	
		vmloadLogDataset.show();
		
		SparkContext sparkContext = session.sparkContext();
		
		vmloadLogDataset.printSchema();
		
		//test RowsBetween
		//val w = Window.partitionBy($"k").orderBy($"Date").rowsBetween(Window.unboundedPreceding, -1)
		WindowSpec window = Window.partitionBy("index").orderBy("index").rowsBetween(-1, 0);
		Dataset<Row> newDs=vmloadLogDataset.withColumn("weightNew", avg(vmloadLogDataset.col("weight")).over(window));
		newDs.show();
		
		//test lag function
		WindowSpec lagWin = Window.orderBy("index");
		Dataset<Row> lagDs = vmloadLogDataset.withColumn("weightLag", lag(vmloadLogDataset.col("weight"), 1).over(lagWin));
		lagDs.show();
		
		//test last 
		WindowSpec lastWin = Window.partitionBy("index").orderBy("index").rowsBetween(-1, 0);
		Dataset<Row> lastDs = lagDs.withColumn("weightLast", last(lagDs.col("weight"), true).over(lastWin));
		lastDs.show();
		
		//test coalesce: returns the first column's value that is not NULL, or NULL if all v's are NULL. This function can take any number of arguments.
		Dataset<Row> coalsceDs = lastDs.withColumn("weightCoalsce", coalesce(lastDs.col("weight"),lastDs.col("weightLast"), lastDs.col("weightLag"), lastDs.col("weight"), lastDs.col("height")));
		coalsceDs.show();
		
		 int maxGap  = 5;
		 List<String> columnsToFill = new ArrayList<String>();
		 columnsToFill.add("weight");
		 String suffix="_";
		 
		 List<Column> laggedColummns = new ArrayList<Column>();
		 WindowSpec simpleWin = Window.orderBy("index");
		 for (String columnName: columnsToFill) {
			 //Generate lag values between 1 and maxGap
			 for (int i =0 ; i<maxGap  ;i++) {
				 laggedColummns.add(lag(vmloadLogDataset.col("weight"), i+1).over(lagWin));
			 } 
		 }
		 Dataset<Row> fillNullDs = vmloadLogDataset.withColumn("weightNoNull", coalesce(vmloadLogDataset.col("weight"), laggedColummns.get(0), laggedColummns.get(1), laggedColummns.get(2)));
		 fillNullDs.show();
		 
		 
		 Dataset<Row> noNullWeight = fillNullValues(vmloadLogDataset, "index", "weight");
	}
	
	public Dataset<Row> fillNullValues(Dataset<Row> dataset, String orderByColumn, String columnName) {
		//val w = Window.orderBy($"index")
		//df.withColumn("noNullWeight", last($"weight", true).over(w)))
		WindowSpec lastWin = Window.orderBy(orderByColumn);	
		Dataset<Row> noNullDs= dataset.withColumn("noNull"+columnName, last(dataset.col(columnName),true).over(lastWin));
		noNullDs.show();
		
		return noNullDs;
	}
}
