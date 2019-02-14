package com.sap.hadoop.test.kafka;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

public class KafkaProducerRepeatively {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of kafka progrm");

		Producer producer = createNewProducerDefault();
		System.out.println("Producer created " + producer.toString());
		
		
		produce(producer, "AdRealTimeLog",100);

	}

	private static Producer createNewProducerDefault() throws Exception {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "10.169.68.26:9092");

		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", "0");
		kafkaProps.put("linger.ms", "1");

		kafkaProps.put("linger.ms", "1");
		//kafkaProps.put("partitioner.class", "com.sap.hadoop.test.kafka.SimplePartition");

		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer producer = new KafkaProducer<String, String>(kafkaProps);
		return producer;
	}
	
	private static void produce(Producer producer, String topic, int count) throws Exception {
		 System.out.println("Start to send topics");
		 for (int i = 1; i <= count; i++) {
	            String value = generateRandomLog();
	            ProducerRecord<String, String> msg = new ProducerRecord<String, String>(topic, String.valueOf(i), value);
	            System.out.println("Start to send msg " + msg);
	            producer.send(msg);
	            System.out.println("Sent msg " + msg);
	            Thread.sleep(2000);
	        }
	        //list topics information
	        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
	        partitions = producer.partitionsFor(topic);
	        for(PartitionInfo p:partitions)
	        {
	            System.out.println(p);
	        }
	}
	
	//Format: 
	 //timestamp	1450702800 
	 //province 	Jiangsu	
	 //city 	Nanjing
	 //userid  0-99
	 //adid 	 10001-10050
	private static String generateRandomLog() {
		
		String[] provinces = {"Hubei" ,"Jiangsu", "Hebei", "Guizhou", "Zhejiang", "Jiangxi"};
		String[] cities = {"Wuhan" ,"Nanjing", "Shijiazhuang", "Guiyang", "Hangzhou", "Nanchang"};
		
		StringBuffer logBuf = new StringBuffer();
		Random random = new Random();
		
		Date now = new Date();
		int rndDiff = random.nextInt(10);
		Date randomDate = new Date(now.getTime() - rndDiff * 1000 * 24 * 3600);
		
		int rndProvIndex = random.nextInt(5);
		
		logBuf.append(randomDate.getTime());
		logBuf.append(" ");
		
		logBuf.append(provinces[rndProvIndex]);
		logBuf.append(" ");
		
		logBuf.append(cities[rndProvIndex]);
		logBuf.append(" ");		
		
		logBuf.append(20+rndProvIndex);
		logBuf.append(" ");		

		logBuf.append(1000 + random.nextInt(99));
		logBuf.append(" ");		
		return logBuf.toString();
	}
}
