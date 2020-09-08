package flink.client.flinkClientTest;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

public class FlinkKafkaClient {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of the Flink Kafka Progogram");
		testKafka();
		

	}
	
	
	public void testSimpleKafka1() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.116.29.53:9092");
		properties.setProperty("group.id", "gclogs");

		DataStream<String> stream = env
				.addSource(new FlinkKafkaConsumer<>("monsoongcverboselogs", new SimpleStringSchema(), properties));

		AllWindowedStream<String, TimeWindow> result = stream.map(data -> {
			System.out.println("data received " + data);
			return data;
		}).timeWindowAll(Time.seconds(5));
	}
	
	public static void testKafka() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.116.29.53:9092");
		properties.setProperty("group.id", "gclogs");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("monsoongcverboselogs", new SimpleStringSchema(), properties);
 
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);
        //stream.print();
        DataStream<String> mappedStream = stream.map(new MapFunction<String, String>() {
			@Override
			public String map(String value) throws Exception {
				System.out.println("get json format msg:" + value);
				
				return value;
			}
        	
        });
        
        writeToEs(true,mappedStream);
        
        env.execute("good environment execution");
	}
	
	public static void writeToEs(boolean writeToElasticsearch, DataStream<String> mappedStream) {
	    if(writeToElasticsearch){
	    	List<HttpHost> httpHosts = new ArrayList<>();
	    	httpHosts.add(new HttpHost("10.116.29.96", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.28.152", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.31.203", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.29.5", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.30.5", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.30.192", 9200, "http"));
	    	httpHosts.add(new HttpHost("10.116.29.96", 9200, "http"));

	    	// use a ElasticsearchSink.Builder to create an ElasticsearchSink
	    	ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<String>(httpHosts,
	    	    new ElasticsearchSinkFunction<String>() {
	    	        public IndexRequest createIndexRequest(String element) {
	    	            Map<String, String> json = new HashMap<>();
	    	            json.put("data", element);
	    	            Date date = new Date();
	    	            
	    	            String esTimeStampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	    	            SimpleDateFormat format = new SimpleDateFormat(esTimeStampFormat);
	    	            String dateString = format.format( date );
	    	            
	    	            json.put("timestamp", dateString);
	    	            
	    	            return Requests.indexRequest()
	    	                    .index("monsoonpwlogsfromflk")
	    	                    .type("my-type")
	    	                    .source(json);
	    	        }
	    	        
	    	        @Override
	    	        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
	    	            indexer.add(createIndexRequest(element));
	    	        }
	    	    }
	    	);

	    	// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
	    	esSinkBuilder.setBulkFlushMaxActions(1);


	    	// finally, build and add the sink to the job's pipeline
	    	esSinkBuilder.setBulkFlushMaxActions(1);
            mappedStream.addSink(esSinkBuilder.build());
        }
	}

}
