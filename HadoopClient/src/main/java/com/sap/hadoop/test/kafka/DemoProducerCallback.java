package com.sap.hadoop.test.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class DemoProducerCallback implements Callback {
	@Override
	public void onCompletion(RecordMetadata recordMetadata, Exception e) {
		System.out.println("partition " + recordMetadata.partition() + " offset " + recordMetadata.offset());
		if (e != null) {
			e.printStackTrace();
		}
	}
}