package com.sap.hadoop.client.spark.perfload.logs;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import com.sap.hadoop.client.spark.CommonUtil;

import scala.Tuple2;

public class SparkProcessLogs {
	
	
	public static Dataset<Row> parseMultipleFiles(SparkSession session, String logFiles, String logType) {
		String[] logPaths = logFiles.split(";");
		List<Dataset<Row>> list = new ArrayList<Dataset<Row>>();
		for (int i=0 ; i<logPaths.length; i++) {
			Dataset<Row> eachRow = null;
			if (logType.equalsIgnoreCase("nginxlog")) {
				eachRow = parseNginxLogs(session, logPaths[i]);
			} else if (logType.equalsIgnoreCase("perflog")) {
				eachRow = parsePerflog(session, logPaths[i]);
			} else if (logType.equalsIgnoreCase("serverlog")) {
				eachRow = parseServerLogs(session, logPaths[i]);
			}
			list.add(eachRow);
		}
		
		if(list.size() == 1) {
			return list.get(0);
		} else {
			Dataset<Row> initial = list.get(0);
			for (int j=1; j<list.size(); j++) {
				initial = initial.union(list.get(j));
			}
			return initial;
		}
	}
	
	public static Dataset<Row> parsePerflog(SparkSession session, String perflogCSVPath) {
		Dataset<Row> perfLogDataset = session.read()
				.csv(perflogCSVPath);

		List<Row> top5 = perfLogDataset.takeAsList(5);
		for (Row eachRow : top5) {
			int size = eachRow.size();
			System.out.print("each row:");
			for (int i = 0; i < size; i++) {
				System.out.print(eachRow.get(i) + "|");
			}
			System.out.println(" ");
		}

		System.out.println("perflog count " + perfLogDataset.count());

		//CommonUtil.printTop5RowsOfDataset("./top5rowsPerflog", perfLogDataset);

		// Convert perflog data set to key value pairs data set
		StructType structType = new StructType();
		structType = structType.add("dateTime", DataTypes.StringType, true);
		structType = structType.add("costTime", DataTypes.IntegerType, true);
		structType = structType.add("PLV", DataTypes.StringType, true);
		structType = structType.add("CIP", DataTypes.StringType, true);
		structType = structType.add("CMID", DataTypes.StringType, true);
		structType = structType.add("CMN", DataTypes.StringType, true);
		structType = structType.add("SN", DataTypes.StringType, true);
		structType = structType.add("DPN", DataTypes.StringType, true);
		structType = structType.add("UID", DataTypes.StringType, true);
		structType = structType.add("UN", DataTypes.StringType, true);
		structType = structType.add("LOC", DataTypes.StringType, true);
		structType = structType.add("EID", DataTypes.StringType, true);
		structType = structType.add("AGN", DataTypes.StringType, true);
		structType = structType.add("RID", DataTypes.StringType, true);
		structType = structType.add("MTD", DataTypes.StringType, true);
		structType = structType.add("URL", DataTypes.StringType, true);
		structType = structType.add("RQT", DataTypes.StringType, true);

		structType = structType.add("MID", DataTypes.StringType, true);
		structType = structType.add("PID", DataTypes.StringType, true);
		structType = structType.add("PQ", DataTypes.StringType, true);
		structType = structType.add("MEM", DataTypes.StringType, true);
		structType = structType.add("CPU", DataTypes.StringType, true);
		structType = structType.add("UCPU", DataTypes.StringType, true);
		structType = structType.add("SCPU", DataTypes.StringType, true);
		structType = structType.add("FRE", DataTypes.StringType, true);
		structType = structType.add("FWR", DataTypes.StringType, true);
		structType = structType.add("NRE", DataTypes.StringType, true);
		structType = structType.add("NWR", DataTypes.StringType, true);
		structType = structType.add("SQLC", DataTypes.StringType, true);
		structType = structType.add("SQLT", DataTypes.StringType, true);
		structType = structType.add("RPS", DataTypes.StringType, true);
		structType = structType.add("SID", DataTypes.StringType, true);
		structType = structType.add("GID", DataTypes.StringType, true);
		structType = structType.add("STK", DataTypes.StringType, true);

		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

		Dataset<Row> detailedRow = perfLogDataset.map((MapFunction<Row, Row>) originalRow -> {
			Row newRow = null;
			if (originalRow.size() < 2) {
				return null;
			}
			//CommonUtil.writeLineToFile("perfLogDataset", " inside perfLogDataset " + originalRow);
			String line = originalRow.getString(1);

			int timeCost = Integer.parseInt(line.substring(0, 4).trim());
			// List<String> matchedPairs = CommonUtil.captureMatchedStringList(value,
			// "([0-9]+)", -1);
			String remainingLine = line.substring(5).trim();

			String stk = null;
			if (remainingLine.indexOf("STK=") >= 0) {
				stk = remainingLine.substring(remainingLine.indexOf("STK=") + 4);
				remainingLine = remainingLine.substring(0, remainingLine.indexOf("STK=") - 1);
			}
			
			//System.out.println("remaining line after STK: " + remainingLine);
			remainingLine = remainingLine + " WDF=1234 PWAN=12345";
			
			List<String> matchedPairs = CommonUtil.captureMatchedStringList(remainingLine, "([A-Z]+)=([^\"]+?)\\s+", -1);
			//List<String> matchedPairs = CommonUtil.captureMatchedStringList(remainingLine, "([A-Z]+)=([^\"^\\s]+?)\\s+", -1);
			
			List<String> matchedPairsQuotes = CommonUtil.captureMatchedStringList(remainingLine,
					"([A-Z]+)=\"(.+?)\"\\s+", -1);
			
			matchedPairs.addAll(matchedPairsQuotes);
			HashMap propMap = new HashMap();
			for (String pair : matchedPairs) {
				String[] pairArray = pair.split("=");
				String value = pairArray[1];
				if (pairArray[1].startsWith("\"")) {
					value = pairArray[1].substring(1, pairArray[1].length() - 2);
				}
				propMap.put(pairArray[0], value.trim());
			}

			newRow = RowFactory.create(originalRow.get(0), timeCost, propMap.get("PLV"), propMap.get("CIP"),
					propMap.get("CMID"), propMap.get("CMN"), propMap.get("SN"), propMap.get("DPN"), propMap.get("UID"),
					propMap.get("UN"), propMap.get("LOC"), propMap.get("EID"), propMap.get("AGN"), propMap.get("RID"),
					propMap.get("MTD"), propMap.get("URL"), propMap.get("RQT"), propMap.get("MID"), propMap.get("PID"),
					propMap.get("PQ"), propMap.get("MEM"), propMap.get("CPU"), propMap.get("UCPU"), propMap.get("SCPU"),
					propMap.get("FRE"), propMap.get("FWR"), propMap.get("NRE"), propMap.get("NWR"), propMap.get("SQLC"),
					propMap.get("SQLT"), propMap.get("RPS"), propMap.get("SID"), propMap.get("GID"), stk);
			return newRow;
		}, encoder);

		//CommonUtil.printTop5RowsOfDataset("./detailedPerflog", detailedRow);
		
		return detailedRow;
	}
	
	public static Dataset<Row> parseNginxLogs(SparkSession session, String nginxlogCSVPath) {
		Dataset<Row> nginxLogDataset = session.read().csv(nginxlogCSVPath);

		//System.out.println(" processNginxLogs nginx count " + nginxLogDataset.count());
		// CommonUtil.printTop5RowsOfDataset("./top5rowsNginxlog", nginxLogDataset);

		// parse Nginx log row
		StructType structType = new StructType();
		structType = structType.add("remoteAddr", DataTypes.StringType, true);
		structType = structType.add("proxyAddr", DataTypes.StringType, true);
		structType = structType.add("remoteUser", DataTypes.StringType, true);
		structType = structType.add("timeLocal", DataTypes.TimestampType, true);

		structType = structType.add("requestURL", DataTypes.StringType, true);
		structType = structType.add("status", DataTypes.StringType, true);
		structType = structType.add("bytesSent", DataTypes.DoubleType, true);
		structType = structType.add("httpReferer", DataTypes.StringType, true);
		structType = structType.add("httpUserAgent", DataTypes.StringType, true);
		structType = structType.add("jSessionId", DataTypes.StringType, true);
		structType = structType.add("GId", DataTypes.StringType, true);
		structType = structType.add("companyId", DataTypes.StringType, true);
		structType = structType.add("requestTime", DataTypes.DoubleType, true);
		structType = structType.add("requestLength", DataTypes.DoubleType, true);

		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

		Dataset<Row> detailedRow = nginxLogDataset.map((MapFunction<Row, Row>) originalRow -> {
			Row newRow = null;

			String rowStr = originalRow.getString(0);
			String remoteAddr = CommonUtil.extractIpFromString(rowStr);
			//CommonUtil.writeLineToFile("processNginxLogsDebug", " get remote Addr " + remoteAddr);

			String remainingStr = CommonUtil.extractSubString(rowStr, remoteAddr);
			String proxyAddr = CommonUtil.extractIpFromString(remainingStr.trim());
			//CommonUtil.writeLineToFile("processNginxLogsDebug", " proxy str  " + proxyAddr);
			String remainingStr2 = CommonUtil.extractSubString(remainingStr, proxyAddr);
			//CommonUtil.writeLineToFile("processNginxLogsDebug", " remainingStr2  " + remainingStr2);

			String replaceBracket = remainingStr2.replaceAll("\\[", "\"");
			replaceBracket = replaceBracket.replaceAll("\\]", "\"");

			List<String> list = CommonUtil.splitBySpaceOrQuote(replaceBracket);
			for (String str : list) {
				CommonUtil.writeLineToFile("processNginxLogsDebug", "each one:" + str);
			}
			if (list.size() >= 13) {
				//Format date time such as: 18/Jul/2019:09:09:48 +0000
				Date logDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss +0000").parse(CommonUtil.removeQuote(list.get(2)));
				java.sql.Timestamp logTime = new Timestamp(logDate.getTime());
				java.sql.Date logSQLDate = new java.sql.Date(logDate.getTime());
				
				double bytesSent = 0;
				double requestTime = 0;
				double requestLength = 0;
				try { 
					bytesSent =  Double.parseDouble(list.get(5));
					requestTime = Double.parseDouble(list.get(11));
					requestLength = Double.parseDouble(list.get(12));
				} catch (NumberFormatException e) {
					e.printStackTrace();
					System.err.println("error when parse double " + e.getMessage());
				}

				
				newRow = RowFactory.create(remoteAddr, proxyAddr, list.get(0), logTime,
						CommonUtil.removeQuote(list.get(3)), list.get(4), bytesSent,
						CommonUtil.removeQuote(list.get(6)), CommonUtil.removeQuote(list.get(7)),
						CommonUtil.removeQuote(list.get(8)), CommonUtil.removeQuote(list.get(9)),
						CommonUtil.removeQuote(list.get(10)), requestTime, requestLength);
			} else {
				System.out.println("bad size " + list.size());
			}
			return newRow;
		}, encoder);
		// Print schema definition
		detailedRow.printSchema();

		// Filter the Null row
		detailedRow = detailedRow.filter(row -> row != null);
		
		return detailedRow;
	}
	
	public static Dataset<Row> parseServerLogs(SparkSession session, String serverlogCSVPath) {
		Dataset<Row> serversLogDataset = session.read().csv(serverlogCSVPath);

		//CommonUtil.printTop5RowsOfDataset("./top5rowsServerslog", serversLogDataset);

		// parse server log row
		StructType structType = new StructType();
		structType = structType.add("dateTime", DataTypes.StringType, true);
		ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);

		Dataset<Row> detailedRow = serversLogDataset.map((MapFunction<Row, Row>) originalRow -> {
			Row newRow = null;
			//CommonUtil.writeLineToFile("serversLogDataset", "originalRow " + originalRow);
			String start = originalRow.getString(0);
			String regex = "^[0-9]{2}:[0-9]{2}:[0-9]{2}";
			if (CommonUtil.checkPatternMatch(regex, start)) {
				return originalRow;
			}
			return null;
		}, encoder).filter(row -> {
			// Filter the empty lines.
			return (row != null);
		});

		//1. First choice, map to Tuple
		Encoder<Tuple2<String, Tuple2<String, String>>> tupleEncoder = Encoders.tuple(Encoders.STRING(),
				Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
		Dataset<Tuple2<String, Tuple2<String, String>>> tupleRows = detailedRow.map(row -> {
			String timeStamp = row.getString(0);
			String rowStr = row.toString();

			String regex = "\\s*\\[?\\d{1,2}:\\d{1,2}:\\d{1,2}\\,\\d{1,3}";
			String matchTime = CommonUtil.matchString(rowStr, regex);
			String remainStr = CommonUtil.cutString(rowStr, matchTime);

			String infoReg = "\\s*\\S+\\s*";
			String matchDebugInfo = CommonUtil.matchString(remainStr, infoReg).trim();
			String remainStr2 = CommonUtil.cutString(remainStr, matchDebugInfo);

			String funcReg = "\\s*\\[\\S+\\]\\s*";
			String matchFunc = CommonUtil.matchString(remainStr2, funcReg);

			String remainStr3 = CommonUtil.cutString(remainStr2, matchFunc);
			String ipReg = "\\s*\\[\\S+\\]\\s*";
			String matchIp = CommonUtil.matchString(remainStr3, ipReg);

			String remainStr4 = CommonUtil.cutString(remainStr3, matchIp);
			String countReg = "\\s*\\[\\S+\\]\\s*";
			String matchCount = CommonUtil.matchString(remainStr4, countReg);

			String remainStr5 = CommonUtil.cutString(remainStr4, matchCount);
			String dbInfoReg = "\\s*\\[\\S+\\]\\s*";
			String matchDBInfo = CommonUtil.matchString(remainStr5, dbInfoReg);

			String remainStr6 = CommonUtil.cutString(remainStr5, matchDBInfo);

			String key = CommonUtil.trimBracket(matchTime) + "_" + matchDebugInfo.trim() + "_"
					+ CommonUtil.trimBracket(matchIp);

			String tupleKey = CommonUtil.trimBracket(matchFunc) + "==" + CommonUtil.trimBracket(matchCount) + "=="
					+ CommonUtil.trimBracket(matchDBInfo);

			String tupleValue = CommonUtil.trimBracket(remainStr6);

			return new Tuple2<String, Tuple2<String, String>>(key, new Tuple2<String, String>(tupleKey, tupleValue));
		}, tupleEncoder);
		Dataset<Row> tupleServerRowsKeyValues = tupleRows.toDF("key", "value");
		//CommonUtil.printTop5RowsOfDataset("./tupleServerRowsKeyValues", tupleServerRowsKeyValues);

		//2. Second choice, map to Row
		StructType structTypeSereverlog = new StructType();
		structTypeSereverlog = structTypeSereverlog.add("logTime", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("debugOrInfo", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("ipAddr", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("function", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("count", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("GID", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("dbInfo", DataTypes.StringType, true);
		structTypeSereverlog = structTypeSereverlog.add("messages", DataTypes.StringType, true);
	
		ExpressionEncoder<Row> encoderServerRow = RowEncoder.apply(structTypeSereverlog);
		Dataset<Row> rowServerLogs = detailedRow.map(row -> {
			String timeStamp = row.getString(0);
			String rowStr = row.toString();

			String regex = "\\s*\\[?\\d{1,2}:\\d{1,2}:\\d{1,2}\\,\\d{1,3}";
			String matchTime = CommonUtil.matchString(rowStr, regex);
			String remainStr = CommonUtil.cutString(rowStr, matchTime);

			String infoReg = "\\s*\\S+\\s*";
			String matchDebugInfo = CommonUtil.matchString(remainStr, infoReg).trim();
			String remainStr2 = CommonUtil.cutString(remainStr, matchDebugInfo);

			String funcReg = "\\s*\\[\\S+\\]\\s*";
			String matchFunc = CommonUtil.matchString(remainStr2, funcReg);

			String remainStr3 = CommonUtil.cutString(remainStr2, matchFunc);
			String ipReg = "\\s*\\[\\S+\\]\\s*";
			String matchIp = CommonUtil.matchString(remainStr3, ipReg);

			String remainStr4 = CommonUtil.cutString(remainStr3, matchIp);
			String countReg = "\\s*\\[\\S+\\]\\s*";
			String matchCount = CommonUtil.matchString(remainStr4, countReg);

			String remainStr5 = CommonUtil.cutString(remainStr4, matchCount);
			String GIDReg = "\\s*\\[\\S+\\]\\s*";
			String GIDStr = CommonUtil.matchString(remainStr5, GIDReg);
			
			String remainStr51 = CommonUtil.cutString(remainStr5, GIDStr);
			String dbInfoReg = "\\s*\\[\\S+\\]\\s*";
			String matchDBInfo = CommonUtil.matchString(remainStr51, dbInfoReg);

			String remainStr6 = CommonUtil.cutString(remainStr5, matchDBInfo);

			return RowFactory.create(CommonUtil.trimBracket(matchTime),matchDebugInfo.trim(),  CommonUtil.trimBracket(matchIp), CommonUtil.trimBracket(matchFunc), 
					CommonUtil.trimBracket(matchCount),CommonUtil.trimBracket(GIDStr),CommonUtil.trimBracket(matchDBInfo),CommonUtil.trimBracket(remainStr6));
		}, encoderServerRow);
		//CommonUtil.printTop5RowsOfDataset("./rowServerLogs", rowServerLogs);
		return rowServerLogs;
	}
}
