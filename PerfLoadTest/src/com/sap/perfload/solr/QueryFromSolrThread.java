package com.sap.perfload.solr;

import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

import com.sap.perfload.util.CommonUtil;

public class QueryFromSolrThread implements Runnable {

	private String solrUrl;
	private String name;
	private Map valuesMap;
	private static Logger logger = Logger.getLogger(QueryFromSolrThread.class);

	public QueryFromSolrThread(String name, Map valuesMap, String solrUrl) {

		super();
		this.solrUrl = solrUrl;
		this.name = name;
		this.valuesMap = valuesMap;
	}

	// Letter frequency etaoinsrhldcumfp g w y b v k x j q z
	@Override
	public void run() {
		try {
			String company = (String) this.valuesMap.get("companyId");
			String qLetters = (String) this.valuesMap.get("qLetters");

			if (qLetters == null) {
				qLetters = "etaoinsrhldcumfp";
			}
			// String letters = "etaoinsrhldcumfp";
			String urlString = solrUrl + company;
			SolrClient solr = new HttpSolrClient.Builder(urlString).build();

			String q = (String) this.valuesMap.get("q");
			String field = (String) this.valuesMap.get("field");
 
			Object startObj = this.valuesMap.get("start");
			Integer start = 0;
			if (startObj != null) {
				start = Integer.parseInt((String) startObj);
			}
		  
			Boolean facet = false;
			if ("true".equalsIgnoreCase((String)this.valuesMap.get("facet"))) {
				facet = true;
			}

			int rows = Integer.parseInt((String) this.valuesMap.get("rows"));
			int randomLetters = Integer.parseInt((String) this.valuesMap.get("randomLetters"));
			int times = Integer.parseInt((String) this.valuesMap.get("times"));
			
			logger.debug("valuesMap  qLetters " + qLetters + " randomLetters " + randomLetters + " company " + company +  " times " +times);
			
			for (int i = 0; i < qLetters.length() * times; i++) {
				// letters.substring(0,1);
				SolrQuery query = new SolrQuery();
				String queryStr = q + ":" + "*" + CommonUtil.generateRandomString(qLetters, randomLetters, "*");					

				query.setQuery(queryStr);
				query.addField(field);
				query.setRows(rows);
				query.setStart(start);
				query.setFacet(facet);

				QueryResponse queryResponse;
				queryResponse = solr.query(query);
				SolrDocumentList docs = queryResponse.getResults();
				logger.debug("Query " + queryStr + " got size " + docs.size());
				// System.out.println("Query " + queryStr + " got size " + docs.size());
				// solr.commit();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
