package com.mehmood86.kafka.service;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TopicSeekToBeginning {

	@Value("${spring.kafka.topic-name}")
	private String topic;

	@Autowired
	KafkaConsumer<String, String> consumer;

	public void getAllMessages() {

//		consumer.subscribe(Collections.singletonList(topic));
//		consumer.poll(Duration.ofMillis(100)); // without this, the assignment will be empty.
//		consumer.assignment().forEach(t -> {
//			System.out.printf("Set %s to offset 0%n", t.toString());
//			consumer.seek(t, 0);
//		});

//	    while (true) {
//			var records = consumer.poll(Duration.ofMillis(100));
//
//			for (var record : records) {
//				System.out.println("record value: " + record.value());
//			}
//			
//
//			consumer.commitAsync();
//		}

		consumer.subscribe(Set.of(topic), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				consumer.seekToBeginning(partitions);
			}
		});

		while(consumer.assignment().isEmpty()) {
			consumer.poll(Duration.ofMillis(0));
			try {
				System.out.println("consumer assignment: " + consumer.assignment());
				Thread.sleep(500);
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
		}
		while (consumer.assignment().isEmpty()) {
			
			var records = consumer.poll(Duration.ofMillis(50));

			for (var record : records) {
				System.out.println("record value: " + record.value());
			}
			

			consumer.commitAsync();
		}

	}
}
