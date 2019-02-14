package com.sap.hadoop.test.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
	private ProcessorContext context = null;

	@Override
	public void close() {

	}

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public void process(byte[] key, byte[] value) {
		String strOrigin = new String(value);
		if (strOrigin.contains(">>>")) {
			strOrigin = strOrigin.split(">>>")[1];
		}
		context.forward(key, strOrigin.getBytes());
	}

}
