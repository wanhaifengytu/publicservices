package com.sap.hadoop.test.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducerTest {
	
	public static void main(String[] args) throws Exception {
		Properties props = new Properties();
		 props.put("bootstrap.servers", "10.169.68.26:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for (int i = 0; i < 300; i++) {
			 
			 String msg = generateRandomLog();
		     producer.send(new ProducerRecord<String, String>("AdRealTimeLog", Integer.toString(i), msg));
		     System.out.println("Sent " + msg);
		     Thread.sleep(2000);
		 }
		 producer.close();
	}
	
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
		
		logBuf.append(20 + rndProvIndex);
		logBuf.append(" ");		

		logBuf.append(1000 + random.nextInt(99));
		logBuf.append(" ");		
		return logBuf.toString();
	}
}
