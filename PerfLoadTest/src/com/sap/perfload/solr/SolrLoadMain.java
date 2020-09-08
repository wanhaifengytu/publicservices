package com.sap.perfload.solr;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.sap.perfload.util.CommonUtil;

public class SolrLoadMain {

	private static Logger logger = Logger.getLogger(SolrLoadMain.class);

	public static void main(String[] args) throws Exception {
		Properties props = CommonUtil.loadProperties("solrload.properties");

		String solrUrl = props.getProperty("solrURL").toString();

		ScheduledExecutorService scheduleService = Executors
				.newScheduledThreadPool(Integer.parseInt(props.getProperty("maxThreadPool")));
		List<ScheduledFuture<?>> schedules = new ArrayList<ScheduledFuture<?>>();

		int runningDuration = Integer.parseInt(props.getProperty("runningDuration"));
		if (Integer.parseInt(props.getProperty("queryMultiple").toString()) > 0) {
			// Run query
			callQueryMutiple(props, solrUrl, scheduleService, schedules);
		}

		if (Integer.parseInt(props.getProperty("addMultiple").toString()) > 0) {
			// Run add
			callAddMutiple(props, solrUrl, scheduleService, schedules);
		}

		if (Integer.parseInt(props.getProperty("deleteMultiple").toString()) > 0) {
			// Run delete
			callDeleteMultiple(props, solrUrl, scheduleService, schedules);
		}

		Date oldDate = new Date();
		while (true) {
			Thread.sleep(1000);
			Date newDate = new Date();
			long diff = newDate.getTime() - oldDate.getTime();
			long diffSeconds = diff / 1000 % 60;

			if (diffSeconds >= runningDuration) {
				for (ScheduledFuture<?> eachSchedule : schedules) {
					eachSchedule.cancel(true);
				}
				scheduleService.shutdown();
				break;
			}
		}
		CommonUtil.removeFiles("./", props.getProperty("idFiles").toString());
	}

	/**
	 * 
	 * @param props
	 */
	public static void callQueryMutiple(Properties props, String solrUrl, ScheduledExecutorService scheduleService,
			List<ScheduledFuture<?>> schedules) throws Exception {
		int queryMultiple = Integer.parseInt(props.getProperty("queryMultiple").toString());
		int queryInterval = Integer.parseInt(props.getProperty("queryInterval"));

		Enumeration en = props.propertyNames();
		// Each CacheType spawn a new thread to read from cache
		while (en.hasMoreElements()) {
			String strKey = (String) en.nextElement();
			String strValue = props.getProperty(strKey);
			if (strKey.startsWith("queryInfo")) {
				String[] values = strValue.split("_");
				Map valuesMap = new HashMap();
				for (int j = 0; j < values.length; j++) {
					String eachValue = values[j];
					String keyValues[] = eachValue.split("=");
					if (keyValues.length == 2) {
						valuesMap.put(keyValues[0], keyValues[1]);
					}
				}
				for (int i = 0; i < queryMultiple; i++) {
					Runnable queryWork = new QueryFromSolrThread("QueryFromSolrThread-" + values[0] + "-" + i,
							valuesMap, solrUrl);
					logger.debug("Spawn Thread " + "QueryFromSolrThread-" + values[0] + "-" + i);
					ScheduledFuture<?> scheduledFuture = scheduleService.scheduleAtFixedRate(queryWork, 5,
							queryInterval, TimeUnit.SECONDS);
					schedules.add(scheduledFuture);
				}
			}
		}
	}

	/**
	 * 
	 * @param props
	 * @param solrUrl
	 * @param scheduleService
	 * @param schedules
	 * @throws Exception
	 */
	public static void callAddMutiple(Properties props, String solrUrl, ScheduledExecutorService scheduleService,
			List<ScheduledFuture<?>> schedules) throws Exception {
		int addMultiple = Integer.parseInt(props.getProperty("addMultiple").toString());
		int addInterval = Integer.parseInt(props.getProperty("addInterval"));

		String cores = props.getProperty("addCore").toString();
		String[] coreArray = cores.split(",");

		for (int k = 0; k < coreArray.length; k++) {
			for (int i = 0; i < addMultiple; i++) {
				Runnable addWork = new AddToSolrThread("AddFromSolrThread-" + i, props, solrUrl, i, coreArray[k]);
				logger.debug("Spawn Thread " + "AddFromSolrThread-" + i);
				ScheduledFuture<?> scheduledFuture = scheduleService.scheduleAtFixedRate(addWork, 5, addInterval,
						TimeUnit.SECONDS);
				schedules.add(scheduledFuture);
			}
		}
	}

	public static void callDeleteMultiple(Properties props, String solrUrl, ScheduledExecutorService scheduleService,
			List<ScheduledFuture<?>> schedules) {
		String idFilesPattern = props.getProperty("idFiles").toString();
		// Delete thread num would highly depend on current ids files number.
		File currentDir = new File("./");
		for (File file : currentDir.listFiles()) {
			if (file.getName().startsWith(idFilesPattern)) {
				String fileName = file.getName();
				String[] fileNameStr = fileName.split("_");
				String coreName = fileNameStr[1];
				Runnable deleteWork = new DeleteFromSolrThread("DeleteFromSolrThread " + fileName, props, solrUrl,
						fileName, coreName);
				logger.debug("Spawn Thread " + "DeleteFromSolrThread-" + fileName);
				ScheduledFuture<?> scheduledFuture = scheduleService.schedule(deleteWork, 5, TimeUnit.SECONDS);
				schedules.add(scheduledFuture);
			}
		}
	}
}
