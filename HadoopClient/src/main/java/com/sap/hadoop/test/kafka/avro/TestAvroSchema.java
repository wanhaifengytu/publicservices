package com.sap.hadoop.test.kafka.avro;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.jmx.support.MBeanServerFactoryBean;

import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.management.MBeanServer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestAvroSchema {

	public static void main(String[] args) throws Exception {
		System.out.println("This is the start of test Kafka AVRO schema program");

		Schema schema = new Schema.Parser().parse(new File("test_schema.avsc"));

		Producer producer = createNewProducerDefault();

		int count = 10;

		for (int i = 0; i < count; i++) {
			sendMessage(producer, schema, i);
		}

		producer.close();

		testMBean();
		
		MBeanServerFactoryBean jmsServer = retMbeanServerFactoryBean();
	//	jmsServer.getObject().getAttribute(name, attribute);
	}

	private static void sendMessage(Producer producer, Schema schema, int index)
			throws IOException, InterruptedException, ExecutionException {

		GenericRecord record = (GenericRecord) generate(null, schema);
		System.out.println("Generated Message : " + record);

		int recordCount = 100;

		List<GenericRecord> records = new ArrayList<>();
		for (int i = 0; i < recordCount; i++) {
			records.add(record);
		}

		// Bytes
		byte[] msgBytes = toBytes(schema, records, CodecFactory.nullCodec());

		ProducerRecord<String, byte[]> prodRecord = new ProducerRecord<String, byte[]>(schema.getName() + index,
				msgBytes);
		Future<RecordMetadata> future = producer.send(prodRecord);
		RecordMetadata meta = future.get();
		System.out.println("Record MetaDat  " + meta.partition() + "  " + meta.offset());

	}

	private static Producer createNewProducerDefault() throws Exception {
		Properties kafkaProps = new Properties();
		kafkaProps.put("bootstrap.servers", "localhost:9092");

		kafkaProps.put("acks", "all");
		kafkaProps.put("retries", "0");
		kafkaProps.put("linger.ms", "1");

		// kafkaProps.put("partitioner.class",
		// "com.sap.hadoop.test.kafka.SimplePartition");

		kafkaProps.put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		kafkaProps.put(VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

		Producer<String, byte[]> producer = new KafkaProducer<String, byte[]>(kafkaProps);

		return producer;
	}

	public static Object generate(String target, Schema schema) {
		Random random = new SecureRandom();
		Type type = schema.getType();
		switch (type) {
		case RECORD:
			GenericRecord record = new GenericData.Record(schema);
			for (Schema.Field field : schema.getFields()) {
				record.put(field.name(), generate(field.name(), field.schema()));
			}
			return record;
		case ENUM:
			List<String> symbols = schema.getEnumSymbols();
			return symbols.get(random.nextInt(symbols.size()));
		case ARRAY:
			int length = random.nextInt(5) + 2;
			GenericArray<Object> array = new GenericData.Array(length <= 0 ? 0 : length, schema);
			for (int i = 0; i < length; i++) {
				array.add(generate(target, schema.getElementType()));
			}
			return array;
		case MAP:
			length = random.nextInt(5) + 2;
			Map<Object, Object> map = new HashMap<>(length <= 0 ? 0 : length);
			for (int i = 0; i < length; i++) {
				map.put(UUID.randomUUID(), generate(target, schema.getValueType()));
			}
			return map;
		case UNION:
			List<Schema> types = schema.getTypes();
			List<Schema> realTypes = new ArrayList<>();
			for (Schema t : types) {
				if (t.getType() != Type.NULL) {
					realTypes.add(t);
				}
			}
			return generate(target, realTypes.get(random.nextInt(realTypes.size())));
		case STRING:
			return UUID.randomUUID().toString();
		case BYTES:
			return null;
		case INT:
			return random.nextInt();
		case LONG:
			return random.nextLong();
		case FLOAT:
			return random.nextFloat();
		case DOUBLE:
			return random.nextDouble();
		case BOOLEAN:
			return random.nextBoolean();
		default:
			throw new RuntimeException("Unknown type: " + schema);
		}
	}

	public static byte[] toBytes(Schema schema, List<GenericRecord> records, CodecFactory codec) throws IOException {
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
		ByteArrayOutputStream stream = new ByteArrayOutputStream();

		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

		try {
			dataFileWriter.setCodec(codec);
			dataFileWriter.create(schema, stream);
			for (GenericRecord record : records) {
				dataFileWriter.append(record);
			}
			dataFileWriter.flush();
			return stream.toByteArray();
		} finally {
			dataFileWriter.close();
		}
	}

	public static MBeanServer testMBean() {

		MBeanServerFactoryBean factoryBean = new MBeanServerFactoryBean();
		factoryBean.setLocateExistingServerIfPossible(true);
		factoryBean.afterPropertiesSet();
		return factoryBean.getObject();
	}

	public static MBeanServerFactoryBean retMbeanServerFactoryBean() {

		MBeanServerFactoryBean mBeanServerFactoryBean = new MBeanServerFactoryBean();
		mBeanServerFactoryBean.setLocateExistingServerIfPossible(true);

		return mBeanServerFactoryBean;
	}
}
