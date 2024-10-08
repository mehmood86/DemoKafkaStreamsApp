package com.mehmood86.kafka.service;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class ByteProcessor implements Processor<byte[], byte[], byte[], byte[]> {
	private KeyValueStore<byte[], byte[]> byteStore;

    @Override
    public void init(ProcessorContext context) {
        byteStore = context.getStateStore("byte-store");
    }    

    @Override
    public void close() {
        // No specific cleanup needed for in-memory state store
    }

	@Override
	public void process(Record<byte[], byte[]> record) {
		long timestamp = record.timestamp(); // Access timestamp
        Headers headers = record.headers(); // Access headers

        // Add dummy author header
        headers = headers.add("developer", "Mehmood".getBytes(StandardCharsets.UTF_8));

        // Save the key-value pair to the state store (optional)
        byteStore.put(record.key(), record.value());        
        
    }
}

