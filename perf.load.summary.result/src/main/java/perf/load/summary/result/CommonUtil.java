package perf.load.summary.result;


import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.mean;
import static org.apache.spark.sql.functions.stddev;
import static org.apache.spark.sql.functions.to_timestamp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

public class CommonUtil {

	public static List<String> readFileLine(String filePath) {

		List<String> retList = new ArrayList<String>();
		try {
			File f = new File(filePath);
			if (f.isFile() && f.exists()) {
				InputStream file;
				file = new FileInputStream(f);
				InputStreamReader read = new InputStreamReader(file);
				BufferedReader bufferedReader = new BufferedReader(read);
				String line = null;
				while ((line = bufferedReader.readLine()) != null) {
					retList.add(line);
				}
			} else {
				System.out.print("this file does not exit");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return retList;
	}
	
	public static String[] filterStringArray(String[] originalStrArray, String filterOut) {
		String[] retArray = Arrays.stream(originalStrArray).filter(str ->!filterOut.equalsIgnoreCase(str)).toArray(String[]::new);
		
		return retArray;
	}
	
	/**
	 * 
	 * @param session
	 * @param csvPath
	 */
	public static void changeCSVtoLibSVM(SparkSession session, String csvPath, String labelColumn) {
		
		Dataset<Row> dataset = session.read().option("format","com.databricks.csv").option("header","true").option("inferSchema", "true")
				.csv(csvPath);
		
		StructType schema = dataset.schema();
		scala.collection.immutable.List<StructField> listFileds = schema.toList();
        scala.collection.Iterator<StructField> iterStruct = listFileds.iterator();
        int index = 0;
        while (iterStruct.hasNext()) {
        	StructField structField = iterStruct.next();
        	if (structField.name().equalsIgnoreCase(labelColumn)) {
        		break;
        	}
        	index++;
         }
        final int fIndex = index;
        
		Dataset<String> dsStr = dataset.map((MapFunction<Row, String>) originalRow -> {
			StringBuilder str = new StringBuilder();
			//First get label data
			str.append(originalRow.get(fIndex));
			str.append(" ");
			
			for (int i=0; i<originalRow.size(); i++) {
				Object eachValue = originalRow.get(i);
				if (eachValue!=null && !eachValue.toString().equalsIgnoreCase("0")) {
					str.append(i+1);
					str.append(":");
					str.append(eachValue);
					str.append(" ");
				}
			}
			
			return str.toString();
		}, Encoders.STRING());
		
		//https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io
		dsStr.write().format("com.databricks.spark.csv").option("header", "true").save("vmload2.libsvm");
		dsStr.show();
	}
	
	/**
	 * 
	 * @param session
	 * @param dataSet
	 * @param csvPath
	 */
	public static void saveDatasetToCSV(Dataset<Row> dataSet, String csvPath) {
		
		dataSet.write().format("com.databricks.spark.csv").option("header", "true").save(csvPath);
	}
	
	public static void saveDatasetToElastic(Dataset<Row>  dataset, String indexName) {
		//https://github.com/elastic/elasticsearch-hadoop/issues/1173
		//https://github.com/elastic/elasticsearch-hadoop/issues/1040#

		JavaEsSparkSQL.saveToEs(dataset, indexName);
	}
	
	
	/***
	 * 
	 * @param propertyFile
	 * @return
	 * @throws Exception
	 */
	public static Properties loadProperties(String propertyFile) throws Exception {
		Properties prop = new Properties();
		InputStream input = new FileInputStream(propertyFile);
		prop.load(input);

		return prop;
	}
	
	public static String matchString(String testLine, String regex) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(testLine);
		String matchedStr = "";
		if (matcher.lookingAt()) {
			matchedStr = matcher.group();
		}
		return matchedStr;
	}
	
	public static String cleanUnusedChars(String originalStr) {
		//
		originalStr = originalStr.replaceAll("\\/", " ");
		originalStr = originalStr.replaceAll("\\'", " ");
		originalStr = originalStr.replaceAll("\\:", " ");
		originalStr = originalStr.replaceAll("\\[", " ");
		originalStr = originalStr.replaceAll("\\]", " ");
		originalStr = originalStr.replaceAll("\\(", " ");
		originalStr = originalStr.replaceAll("\\)", " ");
		originalStr = originalStr.replaceAll("\\{", " ");
		originalStr = originalStr.replaceAll("\\}", " ");
		originalStr = originalStr.replaceAll("\\.", " ");
		originalStr = originalStr.replaceAll("\"", " ");
		originalStr = originalStr.replaceAll("\\,", " ");
		originalStr = originalStr.replaceAll("\\=", " ");
		originalStr = originalStr.replaceAll(",", " ");
		
		return originalStr;
	}

	public static String trimBracket(String originalStr) {
		originalStr = originalStr.trim();

		if (originalStr.startsWith("[")) {
			originalStr = originalStr.substring(1);
		}
		if (originalStr.endsWith("]")) {
			originalStr = originalStr.substring(0, originalStr.length() - 1);
		}
		return originalStr;
	}
	
	public static String cutString(String fullStr, String subStr) {
		if (fullStr != null && subStr != null) {
			return fullStr.substring(fullStr.indexOf(subStr) + subStr.length());
		}
		return fullStr;
	}
	
	/**
	 * 
	 * @param fileName
	 * @param data
	 */
	public static void printTop5RowsOfDataset(String fileName, Dataset<Row> data) {
		List<Row> top5 = data.takeAsList(5);
		for (Row  eachRow : top5) {
			CommonUtil.writeLineToFile(fileName,  eachRow.toString());
		}
		
	}
	
	
	/**
	 * 
	 * @param fileName
	 * @param data
	 */
	public static void printAllRowsOfDataset(String fileName, Dataset<Row> data) {
		List<Row> top5 = data.collectAsList();
		for (Row  eachRow : top5) {
			CommonUtil.writeLineToFile(fileName,  eachRow.toString());
		}
		
	}
	
	public static String extractIpFromString(String ipString) {
		String IPADDRESS_PATTERN = 
		        "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";

		Pattern pattern = Pattern.compile(IPADDRESS_PATTERN);
		Matcher matcher = pattern.matcher(ipString);
		if (matcher.find()) {
		    return matcher.group();
		} else{
		    return "0.0.0.0";
		}
	}
	
	public static String extractSubString(String originalStr, String subStr) {
		int index = originalStr.indexOf(subStr);
		
		return originalStr.substring(subStr.length()+1);
	}
	
	public static List<String> splitBySpaceOrQuote(String originalStr) {
		List<String> list = new ArrayList<String>();
		Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(originalStr);
		while (m.find()) {
		    list.add(m.group(1));
		}
		return list;
	}
	
	public static String removeQuote(String originalStr) {
		String retStr = originalStr;
		if (originalStr.startsWith("\"")) {
			retStr = originalStr.substring(1);
		}
	   
		if (retStr.endsWith("\"")) {
			retStr = retStr.substring(0, retStr.length()-1 );
		}
		
		return retStr;
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
	
	/**
	 * Trim the user and scrb information
	 * @param requestURL
	 * @return
	 */
	public static String generalizeURL(String requestURLPart) {
		if (requestURLPart == null) {
			return "";
		}
		if (requestURLPart.indexOf("perfLogServlet") > 0) {
			return "/perfLogServlet";
		}
		
		List<String> matchedURL = CommonUtil.captureMatchedStringList(requestURLPart, "([A-Z]+)\\s+([^\\s]+?)\\s+([^\\s]+)", 2);
		String returnStr = requestURLPart;
		if (matchedURL.size() > 0) {
			returnStr =  matchedURL.get(0);
			List<String> matchedScrb = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)crb=([^\\s]+)", 2);
			if (matchedScrb.size() > 0) {
				returnStr = returnStr.replaceAll(matchedScrb.get(0), "");
			}
			
			List<String> matchedUserInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)user([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedUserInfo.size() > 0) {
				//System.out.println(" Found match " + matchedUserInfo.get(0));
				returnStr = returnStr.replaceAll(matchedUserInfo.get(0), "");
			}
		}
		
		return returnStr;
	}
	
	public static String generalizeURLNew(String requestURL) {
		if (requestURL ==null) {
			return "";
		}
		List<String> matchedURL = CommonUtil.captureMatchedStringList(requestURL, "([^\\s]+)", 1);
		String returnStr = requestURL;
		if (matchedURL.size() > 0) {
			returnStr =  matchedURL.get(0);
			List<String> matchedScrb = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)crb=([^\\s^\\&]+)", 2);
			if (matchedScrb.size() > 0) {
				returnStr = returnStr.replaceAll(matchedScrb.get(0), "");
			}
			
			List<String> matchedUserInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)user([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedUserInfo.size() > 0) {
				returnStr = returnStr.replaceAll(matchedUserInfo.get(0), "");
			}
			
			List<String> matchedCompanyInfo = CommonUtil.captureMatchedStringList(returnStr, "([^\\s]+)company([^\\s^=]*)=([^\\s^\\&]+)", 3);
			if (matchedCompanyInfo.size() > 0) {
				returnStr = returnStr.replaceAll(matchedCompanyInfo.get(0), "");
			}
		}
		return returnStr;
	}

	/**
	 * 
	 * @param regex
	 * @param testStr
	 * @return
	 */
	public static boolean checkPatternMatch(String regex, String testStr) {
		Pattern pattern = Pattern.compile(regex);
		if (pattern != null && testStr != null && testStr.length() > 0) {
			Matcher matcher = pattern.matcher(testStr);
			return matcher.lookingAt();
		}
		return false;

	}
	
	public static String concatGidString(Dataset<Row> dataset) {
		
		List<Row> alldata = dataset.collectAsList();
		StringBuilder strBuilder = new StringBuilder();
		for (int i =0; i< alldata.size() && i <1020; i++) {
			strBuilder.append("\"").append(alldata.get(i).getString(0)).append("\"");
	        strBuilder.append(",");
	     }
		strBuilder.append("\"").append("0").append("\"");

		return strBuilder.toString();
	}
	
	/**
	 * 
	 * @param fileName
	 * @param content
	 */
	public static void writeLineToFile(String fileName, String content) {
		FileWriter fw = null;
		try {
			File f = new File(fileName);
			fw = new FileWriter(f, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		PrintWriter pw = new PrintWriter(fw);

		pw.println(content);
		pw.flush();

		try {
			fw.flush();
			pw.close();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static Vector combineTwoVectors(Vector vecA, Vector vecB) {

		double[] vecAArray = vecA.toArray();
		double[] vecBArray = vecB.toArray();

		double[] oneArray = ArrayUtils.addAll(vecAArray, vecBArray);

		DenseVector denseVec = new DenseVector(oneArray);

		return denseVec;
	}
	
	public static Vector combineTwoVectors(double[] numbers, Vector vec) {

		double[] vecArray = vec.toArray();

		double[] oneArray = ArrayUtils.addAll(numbers, vecArray);

		DenseVector denseVec = new DenseVector(oneArray);

		return denseVec;
	}
	
	public static Dataset<Row> normalizeRow(Dataset<Row> dataset, String column) {
		Row row = dataset.select(mean(dataset.col(column)), stddev(dataset.col(column))).first();
		dataset = dataset.withColumn(column, expr("(" + column + " - " + row.getDouble(0) + ")/" +  row.getDouble(1) + ""));
		return dataset;
	}
}