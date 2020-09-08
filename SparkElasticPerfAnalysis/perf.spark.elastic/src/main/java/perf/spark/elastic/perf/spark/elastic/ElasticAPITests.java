package perf.spark.elastic.perf.spark.elastic;

import java.util.Properties;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;


public class ElasticAPITests {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of elastic API tests");
		
		RestHighLevelClient esClient = getEsRestClient();
		//esClient.
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		SearchRequest searchRequest = new SearchRequest("monsoonperflogs");
		
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		searchRequest.source(searchSourceBuilder);
		
		SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT);
		System.out.println(" res " +response.toString());
		
		esClient.close();
		

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
