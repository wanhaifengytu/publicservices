package com.sap.perfload.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import com.sap.perfload.util.CommonUtil;
import com.sap.perfload.util.HttpRequestUtil;

public class SolrTestApp {

	public static void main(String[] arges) throws Exception {
		System.out.println("This is the start of Solr client test");
		//simpleTry();
		//trySFCompanies();
		
		String testURL = "http://10.116.30.219/search/";

		boolean qryRes = HttpRequestUtil.accessUrl(testURL + "WMLoad/select?q=*%3A*&rows=30");
		System.out.println("results " + qryRes);
		//deleteRecord(testURL);
	    addNewRecord(testURL, 3);
	}

	/**
	 * 
	 */
	public static void addNewRecord(String testURL, int batchSize) throws Exception {
		String urlString = testURL + "perfload1";
		SolrClient client = new HttpSolrClient.Builder(urlString).build();

		for (int i = 0; i < batchSize; i++) {
			SolrInputDocument document = new SolrInputDocument();
			document.setField("id", CommonUtil.generateUniqueKey());
			document.setField("phone", "12345678" + i);
			document.setField("city", "shanghaiabcdefghijklmnop" + i);
			document.setField("first_name", "patricabcdefghijklmno" + i);
			document.setField("last_name", "wan" + i);

			client.add(document);
		}

		client.commit();
		client.close();

	}
	
	
	
	public static void deleteRecord(String testURL) throws Exception {
		String urlString = testURL + "perfload3";
		SolrClient client = new HttpSolrClient.Builder(urlString).build();
		
		client.deleteByQuery("id:156I07UWN4VGQ80J64OL435*");
		client.commit();
		client.close();
	}

	public static void simpleTry() throws Exception {
		String urlString = "http://10.116.29.221:8088/search/solr_sample";
		SolrClient solr = new HttpSolrClient.Builder(urlString).build();

		// Preparing Solr query
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");

		// Adding the field to be retrieved
		query.addField("*");

		// Executing the query
		QueryResponse queryResponse = solr.query(query);

		// Storing the results of the query
		SolrDocumentList docs = queryResponse.getResults();
		System.out.println(docs);
		System.out.println(docs.get(0));
		System.out.println(docs.get(4));
		System.out.println(docs.get(5));

		// Saving the operations
		solr.commit();
	}

	public static void trySFCompanies() throws Exception {

		String urlString = "http://10.116.29.221:8088/search/WMLoad";
		SolrClient solr = new HttpSolrClient.Builder(urlString).build();

		// Preparing Solr query
		SolrQuery query = new SolrQuery();
		query.setQuery("*:*");

		// Adding the field to be retrieved
		query.addField("*");

		// Executing the query
		QueryResponse queryResponse = solr.query(query);

		// Storing the results of the query
		SolrDocumentList docs = queryResponse.getResults();
		System.out.println(docs);
		System.out.println(docs.get(0));
		System.out.println(docs.get(4));
		System.out.println(docs.get(5));

		// Saving the operations
		solr.commit();
	}
}
