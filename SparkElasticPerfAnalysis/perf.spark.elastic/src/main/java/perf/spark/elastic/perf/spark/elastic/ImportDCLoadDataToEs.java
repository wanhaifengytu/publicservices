package perf.spark.elastic.perf.spark.elastic;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.to_timestamp;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class ImportDCLoadDataToEs {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of the import data program");
		
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		
		//importCSVLoadData(sparkSession, "D:\\patric\\dclogs\\loadCPU04t.csv");
		importRealAndPredictData(sparkSession,"D:\\patric\\dclogs\\realvalues.csv");
	}
	
	public static void importRealAndPredictData(SparkSession sparkSession, String csvPath) {
		Dataset<Row> vmloadLogDataset = sparkSession.read().option("format","com.databricks.csv").option("header","true").option("inferSchema", "true")
				.csv(csvPath);
		
		String esTimeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
		vmloadLogDataset = vmloadLogDataset.withColumn("timestampNew", date_format(to_timestamp(col("DateTime"), "MM/dd/yyyy HH:mm:ss"), esTimeStampFormat));
		vmloadLogDataset.printSchema();
		vmloadLogDataset.show();
		
		CommonUtil.saveDatasetToElastic(vmloadLogDataset, "cpuloadprediction");
	}
	
	
	public static void importCSVLoadData(SparkSession sparkSession, String csvPath) {
		Dataset<Row> vmloadLogDataset = sparkSession.read().option("format","com.databricks.csv").option("header","true").option("inferSchema", "true")
				.csv(csvPath);	
		vmloadLogDataset.show();
		
		StructType structType = new StructType();
		structType = structType.add("dateTime", DataTypes.StringType, true);
		structType = structType.add("cpuLoad", DataTypes.DoubleType, true);
		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
		
		Dataset<Row> mappedDataset = vmloadLogDataset.map(row -> {
			String dateStr = row.getString(0);
			
			String strArrays[] = dateStr.split("_");
			
			String myDate= strArrays[0] + " " + strArrays[1] + ":00:00";
			DateFormat iFormatter = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
			DateFormat oFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
			String strDateTime = oFormatter.format(iFormatter.parse(myDate));
			
			double value = row.getDouble(1);
			
			return RowFactory.create(strDateTime,value);
		}, encoder); 
		
		//String esTimeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
		//mappedDataset = mappedDataset.withColumn("dateTimeNew",date_format(to_timestamp(col("dateTime"), esTimeStampFormat), esTimeStampFormat));
		
		mappedDataset.printSchema();
		mappedDataset.show();
		
		CommonUtil.saveDatasetToElastic(mappedDataset, "cpuloaddc4");
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
	
}
