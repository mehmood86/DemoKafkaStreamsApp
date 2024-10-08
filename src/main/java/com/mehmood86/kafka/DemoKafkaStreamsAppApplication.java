package com.mehmood86.kafka;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.mehmood86.kafka.service.KafkaStreamsService;

@SpringBootApplication
public class DemoKafkaStreamsAppApplication {

	public static void main(String[] args) throws IOException {

		ApplicationContext applicationContext = SpringApplication.run(DemoKafkaStreamsAppApplication.class, args);

		// TopicService service = applicationContext.getBean(TopicService.class);
		// topicService.createTopicAndSendMessage();

		//TopicSeekToBeginning service = applicationContext.getBean(TopicSeekToBeginning.class);

		//service.getAllMessages();
		
		/*
		 * Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "enriched-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        StreamsBuilder builder = new StreamsBuilder();
        
        KeyValueStore<byte[], byte[]> byteStore = builder.addStateStore(null).stateStores()
        	    .persistentKeyValueStore("byte-store", Stores.inMemoryKeyValueStoreSerde())
        	    .build();


        // ... Define your processing logic here (refer to point 1) ... // Replace with your processor code

        KafkaStreamsService service = new KafkaStreamsService(builder, props);
        service.startStreams();

        // Retrieve all entries from state store (optional)
        String stateStoreName = "byte-store"; // Replace with your state store name
        ProcessorContext context = null; // Assuming you have access to context
        Map<byte[], byte[]> allEntries = service.getAllFromStateStore(stateStoreName, context);
        System.out.println("State store entries:");
        for (Map.Entry<byte[], byte[]> entry : allEntries.entrySet()) {
            System.out.println("Key: " + new String(entry.getKey(), StandardCharsets.UTF_8));
            System.out.println("Value: " + new String(entry.getValue(), StandardCharsets.UTF_8));
        }
		 */
		
	}

}
