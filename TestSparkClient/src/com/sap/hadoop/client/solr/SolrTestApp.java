package com.sap.hadoop.client.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;

public class SolrTestApp {

	public static void main(String[] arges) throws Exception {
		System.out.println("This is the start of Solr client test");
		simpleTry();
		trySFCompanies();
		
		boolean qryRes = HttpRequestUtil.accessUrl("http://10.116.29.221:8088/search/WMLoad/select?q=*%3A*&rows=30");
		System.out.println("results " + qryRes);
	
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
