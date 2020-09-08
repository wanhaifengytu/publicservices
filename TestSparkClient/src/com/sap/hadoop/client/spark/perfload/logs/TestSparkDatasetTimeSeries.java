package com.sap.hadoop.client.spark.perfload.logs;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.first;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple3;

public class TestSparkDatasetTimeSeries {
	/**
	 * We use a logger to print the output. Sl4j is a common library which works
	 * with log4j, the logging system used by Apache Spark.
	 */
	protected static Logger logger = Logger.getLogger(TestSparkDatasetTimeSeries.class);

	/**
	 * This is the entry point when the task is called from command line with
	 * spark-submit.sh. See
	 * {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\tools");
		String master = "local[*]";
		
		SparkSession session = SparkSession.builder().appName("TestSparkDatasetTimeSeries").master("local").getOrCreate();
		SparkContext sparkContext = session.sparkContext();
		new TestSparkDatasetTimeSeries().processTimeResourcesLogs(session, sparkContext);
		
		//new TestSparkDatasetTimeSeries().processInsuranceData(session, sparkContext);
		
	}
	
	public void processInsuranceData(SparkSession session, SparkContext sparkContext) {
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
		Dataset<Row>  res = session.read().option("header","true").option("inferSchema", "true").csv("D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\FL_insurance_sample.csv");
		res.show();
		
	}
	public void processTimeResourcesLogs(SparkSession session, SparkContext sparkContext) {
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

		Dataset<Row> vmloadLogDataset = session.read().option("format","com.databricks.csv").option("header","true").option("inferSchema", "true")
				.csv("D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\vmload_cf.csv");	
		
		/*StructType structType = new StructType();
		//timestamp,date,tod,r,b,swpd,free,buff,cache,si,so,bi,bo,in,cs,us,sy,id,wa,st
		structType = structType.add("timestamp", DataTypes.StringType, true);
		structType = structType.add("date", DataTypes.IntegerType, true);
		structType = structType.add("tod", DataTypes.StringType, true);
		structType = structType.add("r", DataTypes.StringType, true);
		structType = structType.add("b", DataTypes.StringType, true);
		structType = structType.add("swpd", DataTypes.StringType, true);
		structType = structType.add("free", DataTypes.StringType, true);
		structType = structType.add("buff", DataTypes.StringType, true);
		structType = structType.add("cache", DataTypes.StringType, true);
		structType = structType.add("si", DataTypes.StringType, true);
		structType = structType.add("so", DataTypes.StringType, true);
		structType = structType.add("bo", DataTypes.StringType, true);
		structType = structType.add("in", DataTypes.StringType, true);
		structType = structType.add("cs", DataTypes.StringTyrue);
		structType = structType.add("us", DataTypes.StringType, true);
		structType = structType.add("sy", DataTypes.StringType, true);
		structType = structType.add("id", DataTypes.StringType, true);
		structType = structType.add("wa", DataTypes.StringType, true);
		structType = structType.add("st", DataTypes.StringType, true);
		
		Dataset<Row> vmloadLogDataset = session.read().format("csv")
                .option("sep", ",")
                .option("header", true)
               // .option("mode", "DROPMALFORMED")
                .schema(structType)
                .load("D:\\patric\\tools\\eclipse\\eclipse\\ws\\TestSparkClient\\perflogs\\vmload_cf.csv");
		
		CommonUtil.printTop5RowsOfDataset("./vmloadLogDataset", vmloadLogDataset); */
		
		//Filter function
		Dataset<Row>  filteredByColumn = vmloadLogDataset.filter("us < 70");
		filteredByColumn.show();
		
		//where function
		Dataset<Row>  filteredByWhere = vmloadLogDataset.where("cs < 10000");
		
		//distinct value
		filteredByWhere = filteredByWhere.distinct();
		
		//dropDuplicate by column
		filteredByWhere = filteredByWhere.dropDuplicates("cache","bo");
		filteredByWhere.show();
		
		//sort 
		Dataset<Row>  sortByColumn = filteredByWhere.sort("cache", "bo");
		sortByColumn.show();
		
		//RandomSplit
		Dataset<Row>[] splitsArray = vmloadLogDataset.randomSplit(new double[]{0.9, 0.1});
		splitsArray[0].show();
		splitsArray[1].show();
		
		//Dataset<Row> sampledData=vmloadLogDataset.sample(0.5);
		//sampledData.show();
		
		//drop some columns
		Dataset<Row> dropColumn = vmloadLogDataset.drop("b");
		dropColumn.show();
		
		//add one new column
		Dataset<Row> addOneCOlumn = vmloadLogDataset.withColumn("newst",vmloadLogDataset.col("st"));
		addOneCOlumn.show();
		
		//https://databricks.com/blog/2016/02/09/reshaping-data-with-pivot-in-apache-spark.html
		//actually a kind of Crosstab function
		Dataset<Row> grped = addOneCOlumn.groupBy().pivot("cache").agg(avg(addOneCOlumn.col("bo")));
		grped.show();
		
		Dataset<Row> onlyAgg = addOneCOlumn.agg(avg(addOneCOlumn.col("bo")), max(addOneCOlumn.col("cache")));
		/*Simply get aggregation
		+------------------+----------+
		|           avg(bo)|max(cache)|
		+------------------+----------+
		|108.91108446119581|      3695|
		+------------------+----------+
		*/
		onlyAgg.show();
		
		Dataset<Row> grpThenAggAgg = addOneCOlumn.groupBy(addOneCOlumn.col("bo")).agg(avg(addOneCOlumn.col("bo")), max(addOneCOlumn.col("cache")));
		/*Group then aggregation just like SQL groupby
		 *+----+-------+----------+
		|  bo|avg(bo)|max(cache)|
		+----+-------+----------+
		| 148|  148.0|      3694|
		| 496|  496.0|      3107|
		|1580| 1580.0|      3539|
		|2748| 2748.0|      3353|
		 * 
		 */
		grpThenAggAgg.show();
		
		//rollup: Create a multi-dimensional rollup for the current Dataset using the specified columns
		//rollup is a subset of cube that “computes hierarchical subtotals from left to right”.
		Dataset<Row> rollUpAgg = addOneCOlumn.rollup(addOneCOlumn.col("bo"), addOneCOlumn.col("cache")).agg(avg(addOneCOlumn.col("bo")));
		/*
		 * +----+-----+-------+
			|  bo|cache|avg(bo)|
			+----+-----+-------+
			| 236| 3023|  236.0|
			|   0| 3037|    0.0|
			|  32| 3076|   32.0|
			|  32| 3078|   32.0|
		 * 
		 */
		rollUpAgg.show();
		
	    // add column to ds
        Dataset<Row> newDs = addOneCOlumn.withColumn("newbo",functions.concat(addOneCOlumn.col("bo"), addOneCOlumn.col("cache")));
		
        newDs.show();
		
        //Cube: Create a multi-dimensional cube for the current Dataset using the specified columns, so we can run aggregation on them.
        //The cube function “takes a list of columns and applies aggregate expressions to all possible combinations of the grouping columns”.
        Dataset<Row> cubeAgg = addOneCOlumn.cube(addOneCOlumn.col("bo"), addOneCOlumn.col("cache")).agg(avg(addOneCOlumn.col("bo")));
        cubeAgg.show();
        /*
         * |  bo|cache|avg(bo)|
			+----+-----+-------+
			| 236| 3023|  236.0|
			|   0| 3037|    0.0|
			|  32| 3076|   32.0|
			|  32| 3078|   32.0|
			|   0| 3079|    0.0|
         */
        
        Dataset<Row> describeRows = addOneCOlumn.drop("tod").describe();
        /*
         * Show a wide table
         * +-------+-------------------+--------+------------------+--------------------+------+------------------+-----+------------------+-----+-----+--------------------+------------------+-----------------+------------------+------------------+------------------+------------------+--------------------+-----+-----+
			|summary|          timestamp|     tod|                 r|                   b|  swpd|              free| buff|             cache|   si|   so|                  bi|                bo|               in|                cs|                us|                sy|                id|                  wa|   st|newst|
			+-------+-------------------+--------+------------------+--------------------+------+------------------+-----+------------------+-----+-----+--------------------+------------------+-----------------+------------------+------------------+------------------+------------------+--------------------+-----+-----+
			|  count|              11674|   11674|             11674|               11674| 11674|             11674|11674|             11674|11674|11674|               11674|             11674|            11674|             11674|             11674|             11674|             11674|               11674|11674|11674|
			|   mean|1.560948738945777E9|    null|2.3624293301353436|0.002741134144252...|1990.0|20032.317286277197|548.0|3635.6146993318484|  0.0|  0.0|0.010964536577008738|108.91108446119581|3477.307007024156|2843.6212951858834|11.106390268973788|1.1182970704128834| 87.64322425903718|0.001456227514133...|  0.0|  0.0|
			| stddev| 3371.4101830590776|    null| 6.755331500679598|0.055466436990602316|   0.0|1043.3769312234433|  0.0|130.73848884111032|  0.0|  0.0|   0.295979012287592| 366.4191241344469|6874.365932768908| 3866.608174908602|26.574870696398033| 2.373784398936491|28.811283194693875| 0.03813438718103247|  0.0|  0.0|
			|    min|         1560942900|11:15:00|                 0|                   0|  1990|             19439|  548|              3023|    0|    0|                   0|                 0|              574|              1064|                 0|                 0|                 0|                   0|    0|    0|
			|    max|         1560954578|14:29:38| 
         */
        describeRows.show();
        
        
        //Transform above wide table to be a long table
        describeRows.printSchema();
        
        StructType originalSchema = describeRows.schema();
        scala.collection.immutable.List<StructField> listFileds = originalSchema.toList();
        scala.collection.Iterator<StructField> iterStruct = listFileds.iterator();
        List<StructField> listStruct = new ArrayList<StructField>();
        while (iterStruct.hasNext()) {
        	StructField structField = iterStruct.next();
            listStruct.add(structField);

         }
       
        System.out.println("original schema " + originalSchema); 
        
        JavaSparkContext jSc =  new JavaSparkContext(sparkContext);
        List<Integer> data = new ArrayList<Integer>();
        for(int i=0; i<originalSchema.size()-1; i++) {
        	data.add(new Integer(i+1));
        }
        Dataset<Integer> disDataSet = session.createDataset(data,Encoders.INT());
        disDataSet.show();
        
        Encoder<Tuple3<String, String, Double>> tupleEncoder = Encoders.tuple(Encoders.STRING(),
				Encoders.STRING(), Encoders.DOUBLE());
        
		Dataset<Tuple3<String,String,Double>> detailedRow = describeRows.flatMap((FlatMapFunction<Row, Tuple3<String,String,Double>>) originalRow -> {
			// Get metric as:count, mean, stddev, min, max etc
			String metric = originalRow.getString(0);
			List<Tuple3<String, String, Double>> list = new ArrayList<Tuple3<String, String, Double>>();
			for(int i=1; i<data.size(); i++) {
				String valueActu = originalRow.getString(i);
				if (valueActu ==null) {
					valueActu = "0";
				}
				Tuple3<String, String, Double> tuple3 = new Tuple3<String, String, Double>(metric,
						listStruct.get(i).name(),
						Double.parseDouble(valueActu) );
				list.add(tuple3);
			}

			return list.iterator();
		}, tupleEncoder);
		detailedRow.show();
		
		
		//Transform the long table to wide table
		Dataset<Row> longDFRow = detailedRow.toDF("metric", "field", "value");
		Dataset<Row>  wideRow = longDFRow.groupBy("field")
		         .pivot("metric")
		         .agg(first(longDFRow.col("value")));
		/*
		 * +---------+-------+-------------+--------------------+-----------+--------------------+
			|    field|  count|          max|                mean|        min|              stddev|
			+---------+-------+-------------+--------------------+-----------+--------------------+
			|       us|11674.0|         94.0|  11.106390268973788|        0.0|  26.574870696398033|
			|       st|11674.0|          0.0|                 0.0|        0.0|                 0.0|
			|       in|11674.0|     184470.0|   3477.307007024156|      574.0|   6874.365932768908|
			|     buff|11674.0|        548.0|               548.0|      548.0|                 0.0|
		 */
		wideRow.show();
		
		
	}
}
