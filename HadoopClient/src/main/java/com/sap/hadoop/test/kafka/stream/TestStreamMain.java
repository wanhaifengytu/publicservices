package com.sap.hadoop.test.kafka.stream;

import java.util.Properties;

import org.apache.kafka.streams.StreamsConfig;

public class TestStreamMain {
	
	public static void main(String[] args) {
		System.out.println("This is the start of main program" );
		
		String fromTopic = "test2";
		String toTopic = "test3";
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor"); 
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		
		StreamsConfig config = new StreamsConfig(props);
		//TopologyBuilder builder = new TopologyBuilder();
		
		
		
	}

}
