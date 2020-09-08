package perf.spark.elastic.perf.spark.elastic;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.lang3.math.NumberUtils;

import scala.Tuple2;

public class ProcessDCClickData {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of the import data program");

		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		SparkSession sparkSession = getSparkSession(props);

		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
		String csvPath = (String) props.getProperty("userEventsCSVPath");
		String addStayTimeFilePath = (String) props.getProperty("addStayTimeFilePath");

		importCSVLoadData(sparkSession, csvPath, addStayTimeFilePath);
	}

	// https://stackoverflow.com/questions/34202997/how-to-aggregate-values-into-collection-after-groupby/34204640
	// "_time",CMID,UID,SVT,EET,MID,PID,PQ
	// "2020-08-05T11:05:13.559+0000",infosyspub,INFYTEEJAS,738,3609,HOME,"HOME_TAB","GENERIC_OBJECT_MDF"
	public static void importCSVLoadData(SparkSession sparkSession, String csvPath, String addStayTimeFilePath) {
		Dataset<Row> vmloadLogDataset = sparkSession.read().option("format", "com.databricks.csv")
				.option("header", "true").option("inferSchema", "true").csv(csvPath);

		vmloadLogDataset = vmloadLogDataset.withColumn("comp_user", concat(col("CMID"), col("UID")));

		vmloadLogDataset.show();
		vmloadLogDataset.printSchema();
		JavaRDD<Row> rdd = vmloadLogDataset.rdd().toJavaRDD();

		JavaPairRDD<String, Row> pairRDD = rdd.mapToPair(new PairFunction<Row, String, Row>() {
			@Override
			public Tuple2<String, Row> call(Row row) throws Exception {
				String rowIndx0 = row.getString(1) + "_" + row.getString(2);

				return new Tuple2<String, Row>(rowIndx0, row);
			}
		});
		JavaPairRDD<String, Iterable<Row>> groupedByCompUser = pairRDD.groupByKey();

		int i = 0;
		groupedByCompUser.foreach(row -> {
			// System.out.println("row id " + row._1);
		});

		JavaPairRDD<String,Iterable<String>> sortedInGrps = groupedByCompUser.mapToPair(eachRec -> {
			String key = eachRec._1;
			Iterable<Row> values = eachRec._2;
			List<Row> result = new ArrayList<Row>();
			for (Row row : values) {
				result.add(row);
			}

			result.sort(new Comparator<Row>() {
				@Override //// "2020-08-05T11:05:13.559+0000"
				public int compare(Row m1, Row m2) {
					DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH);

					try {
						Date date1 = format.parse((String) m1.get(0));
						Date date2 = format.parse((String) m2.get(0));
						return date1.compareTo(date2);
					} catch (ParseException e) {
						e.printStackTrace();
					}
					return 1;
				}
			});

			Object[] rows = (Object[]) result.toArray();
			List<String> list = new ArrayList<String>();
			for (int k = 0; k < rows.length; k++) {
				Row rowEach = (Row) rows[k];
				Row nextRow = null;
				DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.ENGLISH);
				
				ClickInfoBean clickBean = new ClickInfoBean();
				
				try {
					Date dateCurrent = format.parse((String) rowEach.get(0));					
					long durationSeconds = 0;
					if (rows.length > 1 && k < rows.length - 1) {
						nextRow = (Row) rows[k + 1];

						if (nextRow != null) {
							Date dateNext = format.parse((String) nextRow.get(0));
							durationSeconds = dateNext.getTime() - dateCurrent.getTime();
						}
					}
					//// "_time",CMID,UID,SVT,EET,MID,PID,PQ
					clickBean.setClickTime(dateCurrent);
					clickBean.setCmid((String) rowEach.get(1));
					clickBean.setUid((String) rowEach.get(2));
					
					int svt = 0;
					int eet = 0;
					if(NumberUtils.isDigits(rowEach.getString(3))) {
						svt = Integer.parseInt(rowEach.getString(3));
					}
					//if(NumberUtils.isDigits((String)rowEach.get(4))) {
						eet =  rowEach.getInt(4);
					//}
					
					clickBean.setSvt(svt);
					clickBean.setEet(eet);
					clickBean.setMid(rowEach.getString(5));
					clickBean.setPid(rowEach.getString(6));
					clickBean.setPq(rowEach.getString(7));
					clickBean.setStayTime(durationSeconds);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				list.add(clickBean.toString());
			}

			Iterable<String> retIter = list;
			return new Tuple2<String, Iterable<String>>(key, retIter);
		});

		/*List<Tuple2<String, Iterable<String>>> selected = sortedInGrps.take(10);
		for (Tuple2<String, Iterable<String>> tuple2: selected) {
			System.out.println("log: key " + tuple2._1);
			Iterable<String> clicks = tuple2._2;
			for(String eachBean: clicks){
				System.out.println(" Click Bean: " + eachBean);
			}
		}*/
		
		/**flat the group rows*/
	   JavaPairRDD<String, String> flatRecords = sortedInGrps.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String,Iterable<String>>, String, String>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, String>> call(
							Tuple2<String, Iterable<String>> tuple) throws Exception {
						List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
						for (String eachOne: tuple._2) {
							Tuple2<String, String> outputTuple = new Tuple2<String, String>(tuple._1,eachOne);	
							list.add(outputTuple);
						}
						return list.iterator();
					}
				});
		
		System.out.println(" flat count " + flatRecords.count());
		List<Tuple2<String, String>> selectedFlat = flatRecords.take(10);
		for (Tuple2<String, String> tuple2: selectedFlat) {
			System.out.println("flat records: key " + tuple2._1 + " "  + tuple2._2);

		}
		flatRecords.repartition(1).saveAsTextFile(addStayTimeFilePath);
		
		flatRecords.collect();

	}

	public static SparkSession getSparkSession(Properties props) throws Exception {
		if (props.getProperty("hadoop.home.dir") != null) {
			System.setProperty("hadoop.home.dir", (String) props.getProperty("hadoop.home.dir"));
		}
		String master = (String) props.getProperty("master");
		SparkConf sparkConf = new SparkConf().setAppName("dcClick").setMaster(master);
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

		return sparkSession;
	}
}
