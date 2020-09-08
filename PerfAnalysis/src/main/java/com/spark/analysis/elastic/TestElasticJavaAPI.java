package com.spark.analysis.elastic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.spark.analysis.logs.CommonUtil;

public class TestElasticJavaAPI {
//https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-getting-started.html
	protected static Logger logger = Logger.getLogger(TestElasticJavaAPI.class);

	public static void main(String[] args) throws Exception {
		System.out.println("TestElasticJavaAPI start main program");
		
		RestHighLevelClient esClient = getEsRestClient();
		//esClient.
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchRequest searchRequest = new SearchRequest("monsoonperflogs");
		
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(" res " +response.toString());
		
		esClient.close();
		
		// using the JDBC Client for Elasticsearch SQL 3 which needs platinum license.
		/*String elasticsearchAddress = "http://10.116.29.96:9200";
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", "");
		connectionProperties.put("password", "");
		String address = "jdbc:es://" + elasticsearchAddress;     

		Connection connection =
		    DriverManager.getConnection(address, connectionProperties);
		
		Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(
              "   SELECT params, logContent, timestamp "
            + "     FROM monsoonserverlogs"
            + " ORDER BY timestamp DESC"
            + "    LIMIT 10s");
       System.out.println(results.next()); */
	
		
		
		
	}

	/**
	 * Highlevel ESClient
	 * 
	 * @return
	 */
	public static RestHighLevelClient getEsRestClient() throws Exception {
		Properties props = CommonUtil.loadProperties("sparkParams.properties");
		String cluster = (String) props.get("esCluster");

		String[] esHosts = cluster.split(",");

		if (esHosts.length >= 3) {
			RestHighLevelClient client = new RestHighLevelClient(
					RestClient.builder(new HttpHost(esHosts[0], 9200, "http"), new HttpHost(esHosts[1], 9200, "http"),
							new HttpHost(esHosts[2], 9200, "http")));
			return client;
		} else {
			return null;
		}
	}

}
