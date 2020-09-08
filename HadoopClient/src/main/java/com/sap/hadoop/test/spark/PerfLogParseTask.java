package com.sap.hadoop.test.spark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import scala.Tuple2;

/**
 * Perflog Parse class, we will call this class with the test WordCountTest.
 */
public class PerfLogParseTask {
	/**
	 * We use a logger to print the output. Sl4j is a common library which works
	 * with log4j, the logging system used by Apache Spark.
	 */
	private static final Logger LOGGER = LoggerFactory.getLogger(PerfLogParseTask.class);

	/**
	 * This is the entry point when the task is called from command line with
	 * spark-submit.sh. See
	 * {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 */
	public static void main(String[] args) {
		// checkArgument(args.length > 0, "Please provide the path of input file
		// as first parameter.");
		String inputFile = "C:\\tools\\Eclipse\\ws\\HadoopClient\\perflogsimple.txt";
		new PerfLogParseTask().run(inputFile);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath) {
		/*
		 * This is the address of the Spark cluster. We will call the task from
		 * WordCountTest and we use a local standalone cluster. [*] means use
		 * all the cores available. See {@see
		 * http://spark.apache.org/docs/latest/submitting-applications.html#
		 * master-urls}.
		 */
		String master = "local[*]";

		/*
		 * Initialises a Spark context.
		 */
		SparkConf conf = new SparkConf().setAppName(PerfLogParseTask.class.getName()).setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(context);

		JavaRDD<String> fileRDD = context.textFile(inputFilePath);

		generateTempPerfLogAllInfoTable(fileRDD, sqlContext);

		generateTempSTKTable(sqlContext);

		fileRDD.count();

		context.close();
	}

	public static void generateTempSTKTable(SQLContext sqlContext) {
		String sql = "select rowKey, stk from tmp_perflog_all_info where stk is not null";

		Dataset<Row> dataset = sqlContext.sql(sql);
		System.out.println("tmp_stk_table count: " + dataset.count());

		List<Row> selectedRows = dataset.javaRDD().take(3);
		System.out.println("SelectedRows count: " + selectedRows.size());
		for(Row row:selectedRows) {
			System.out.println("Row  from tmp_perflog_all_info " + row.get(0)  + " " + row.get(1) );
		}

		JavaRDD<Row> stkRDDRaw = dataset.javaRDD();
		JavaRDD<Tuple2<String, Row>> stkRDD = stkRDDRaw.flatMap(new FlatMapFunction<Row, Tuple2<String, Row>>() {
			@Override
			public Iterator<Tuple2<String, Row>> call(Row row) throws Exception {
				List<Tuple2<String, Row>> oneSTKList = new ArrayList<Tuple2<String, Row>>();
				String rowKey = row.getString(0);
				String stk = (String) row.get(1);
				//System.out.println("stk from row:" + stk);
				JSONObject stkInfoJsonObj = JSONObject.parseObject(stk);
				String name = stkInfoJsonObj.getString("n");
				String iteration = stkInfoJsonObj.getString("i");
				String time = stkInfoJsonObj.getString("t");
				//selfKey, parentKey, n, i , t 
				Row rowParent = RowFactory.create(rowKey, "0", name, iteration, time);
				oneSTKList.add(new Tuple2<String, Row>(rowKey, rowParent));
				
				JSONArray subArray = stkInfoJsonObj.getJSONArray("sub");
				Iterator<Object> it = subArray.iterator();
				int i = 0;
				while (it.hasNext()) {
					JSONObject jsonObj = (JSONObject)it.next();
					String selfKey = rowKey + "_" + String.valueOf(i);
					Row rowSub = RowFactory.create(selfKey,rowKey,  jsonObj.getString("n"), jsonObj.getString("i"), jsonObj.getString("t"));
					oneSTKList.add(new Tuple2<String, Row>(selfKey, rowSub));
				    i++;
				}
				return oneSTKList.iterator();
			}
		});
		
		stkRDD.foreach(tuple -> {
			String rowKey = tuple._1;
			Row row = tuple._2;
			System.out.println("row Key " + rowKey);
		    System.out.println("row " + row.get(0) + " " + row.get(1)+ " " + row.get(2) + " " + row.get(3)+ " " + row.get(4));
		});
     
		stkRDD.count();
	}
	
	public static void traverseJsonObj(JSONObject jsonObj) {
		
		List<Tuple2<String, Row>> oneSTKList = new ArrayList<Tuple2<String, Row>>();
		
		Queue<JSONObject> queue = new LinkedList<JSONObject>();
		//Push the original JsonObject to queue
		queue.offer(jsonObj);
		
		while(!queue.isEmpty()) {
			//If not visited, then visit it.
			JSONObject eachJsonObj = queue.poll();
			{
				
			}
			//Loop the sub elements and put into queue
			JSONArray subArray = eachJsonObj.getJSONArray("sub");
			if (subArray!=null && subArray.size() > 0) {
				Iterator<Object> it = subArray.iterator();
				int i = 0;
				while (it.hasNext()) {
					JSONObject jsonObjEach = (JSONObject)it.next();
				}
			}
		}
		
		//Pop up an elment
		//queue.poll();
	}

	/**
	 * 
	 * @param fileRDD
	 * @param sqlContext
	 */
	public static void generateTempPerfLogAllInfoTable(JavaRDD<String> fileRDD, SQLContext sqlContext) {
		JavaRDD<Row> mappedRowsRDD = fileRDD.map((Function<String, Row>) line -> {
			String dateTime = line.substring(0, 25);
			String remainingLine = line.substring(38).trim();
			String stk = null;
			if (remainingLine.indexOf("STK=") >= 0) {
				stk = remainingLine.substring(remainingLine.indexOf("STK=") + 4);
				remainingLine = remainingLine.substring(0, remainingLine.indexOf("STK=") - 1);
			}
			//System.out.println("stk " + stk);

			List<String> matchedPairs = captureMatchedStringList(remainingLine, "([A-Z]+)=([^\"]+?)\\s+", -1);
			List<String> matchedPairsQuotes = captureMatchedStringList(remainingLine, "([A-Z]+)=\"(.+?)\"\\s+", -1);
			matchedPairs.addAll(matchedPairsQuotes);

			// System.out.println(" matchedPairs " + matchedPairs.size());

			HashMap propMap = new HashMap();
			for (String pair : matchedPairs) {

				String[] pairArray = pair.split("=");
				String value = pairArray[1];
				if (pairArray[1].startsWith("\"")) {
					value = pairArray[1].substring(1, pairArray[1].length() - 2);
				}
				propMap.put(pairArray[0], value.trim());
				// System.out.println("pair " + pairArray[0] + " " + value);
			}

			return RowFactory.create(dateTime.trim() + "_" + propMap.get("CIP"), 
					dateTime.trim(), propMap.get("PLV"), propMap.get("CIP"), propMap.get("CMID"),
					propMap.get("CMN"), propMap.get("SN"), propMap.get("DPN"), propMap.get("UID"), propMap.get("UN"),
					propMap.get("LOC"), propMap.get("EID"), propMap.get("AGN"), propMap.get("RID"), propMap.get("MTD"),
					propMap.get("URL"), propMap.get("RQT"), propMap.get("MID"), propMap.get("PID"), propMap.get("PQ"),
					propMap.get("MEM"), propMap.get("CPU"), propMap.get("UCPU"), propMap.get("SCPU"),
					propMap.get("FRE"), propMap.get("FWR"), propMap.get("NRE"), propMap.get("NWR"), propMap.get("SQLC"),
					propMap.get("SQLT"), propMap.get("RPS"), propMap.get("SID"), stk);
		});

		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("rowKey", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("dateTime", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("plv", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("cip", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("cmid", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("cmn", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sn", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("dpn", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("uid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("un", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("loc", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("eid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("agn", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("rid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("mtd", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("url", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("rqt", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("mid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("pid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("pq", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("mem", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("cpu", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("ucpu", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("scpu", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("fre", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("fwr", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("nre", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("nwr", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sqlc", DataTypes.StringType, true));
		
		structFields.add(DataTypes.createStructField("sqlt", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("rps", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("sid", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("stk", DataTypes.StringType, true));

		StructType schema = DataTypes.createStructType(structFields);
		Dataset<Row> datasetAllInfo = sqlContext.createDataFrame(mappedRowsRDD, schema);

		mappedRowsRDD.count();
		datasetAllInfo.registerTempTable("tmp_perflog_all_info");
	}

	/**
	 * 
	 * @param searchStr
	 * @param regExp
	 * @param groupNum
	 * @return
	 */
	public static List<String> captureMatchedStringList(String searchStr, String regExp, int groupNum) {
		Pattern pattern = Pattern.compile(regExp);
		Matcher matcher = pattern.matcher(searchStr);
		List<String> strList = new ArrayList<String>();

		while (matcher.find()) {
			if (groupNum > 0) {
				strList.add(matcher.group(groupNum));
			} else {
				strList.add(matcher.group());
			}
		}

		return strList;
	}
}