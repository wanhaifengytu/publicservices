package com.sap.hadoop.test.kafka;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerTest {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of Producer Test");
		Producer producer = createNewProducerDefault();
		
		System.out.println("Producer created " + producer.toString());
		
		for (int i=0; i<100; i++) {
				//sendMessageSimple(producer, i);
			    sendAsyncMessage(producer, i);
		}
		Thread.sleep(5500);
	}

	private static Producer createNewProducerDefault() throws Exception {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "10.169.68.198:9092");
		
		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", "0");
		kafkaProps.put("linger.ms", "1");
		
		kafkaProps.put("linger.ms", "1");
		kafkaProps.put("partitioner.class", "com.sap.hadoop.test.kafka.SimplePartition");
		
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer producer = new KafkaProducer<String, String>(kafkaProps);
		return producer;
	}

	private static void sendMessageSimple(Producer producer, int number ) {

		ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products" + number , "France" + number);
		try {
			producer.send(record);
			
			System.out.println("Sent one record " + record);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void sendSyncMessage(Producer producer) {

		ProducerRecord<String, String> record = new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
		try {
			Future future = producer.send(record);
			future.get();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void produceByAvro(String schemaUrl) throws Exception {

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put("schema.registry.url", schemaUrl);

		String schemaString = "{\"namespace\": \"customerManagement.avro\", \"type\": \"record\", "
				+ "\"name\": \"Customer\"," + "\"fields\": [" + "{\"name\": \"id\", \"type\": \"int\"},"
				+ "{\"name\": \"name\", \"type\": \"string\"},"
				+ "{\"name\": \"email\", \"type\": [\"null\",\"string\"], \"default\":\"null\" }" + "]}";
		// Parser parser = new Parser();
		// Schema schema = parser.parse(schemaString);

		String topic = "customerContacts";
		int wait = 500;
		Producer<String, Customer> producer = new KafkaProducer<String, Customer>(props);
		// We keep producing new events until someone ctrl-c
		int i = 0; 
		while (true) {
			Customer customer = new Customer(i, "Patric " + i);
			System.out.println("Generated customer " + customer.toString());
			ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, customer);
			producer.send(record);
		}
	}

	/**
	 * 
	 * @param producer
	 */
	private static void sendAsyncMessage(Producer<String, String> producer, int i) throws Exception {
		ProducerRecord<String, String> record = new ProducerRecord<>("PatricTopic" + (i % 5), "Biomedical Materials" +i, "USA" + i);
		producer.send(record, new DemoProducerCallback());
	}

}
