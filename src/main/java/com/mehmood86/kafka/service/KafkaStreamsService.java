package com.mehmood86.kafka.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;



public class KafkaStreamsService {

    private final StreamsBuilder builder;
    private final Properties props;

    public KafkaStreamsService(StreamsBuilder builder, Properties props) {
        this.builder = builder;
        this.props = props;
    }

    public void startStreams() {
        // ... Define your processing logic here ... // Replace with your processor code

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    public Map<byte[], byte[]> getAllFromStateStore(String stateStoreName, ProcessorContext context) {
        Map<byte[], byte[]> allEntries = new HashMap<>();
        KeyValueStore<byte[], byte[]> stateStore = context.getStateStore(stateStoreName);
        if (stateStore != null) {
            KeyValueIterator<byte[], byte[]> iterator = stateStore.all();
            while (iterator.hasNext()) {
                KeyValue<byte[], byte[]> entry = iterator.next();
                allEntries.put(entry.key, entry.value);
            }
            iterator.close();
        }
        return allEntries;
    }
}

