package com.mehmood86.kafka.service;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class TopicService {

	@Autowired
	private AdminClient adminClient;

	@Value("${spring.kafka.topic-name}")
	private String topic;

	@Autowired
	private KafkaProducer<String, String> producer;

	public void createTopicAndSendMessage() throws IOException {

		if (!createTopicIfNotExist()) {
			System.out.println("Topic '" + topic + "' already exists.");
		}

		sendMessage();
	}

	private boolean createTopicIfNotExist() throws IOException {
		try {
			Collection<TopicListing> topics = adminClient.listTopics().listings().get();
			if (topics.stream().noneMatch(topic -> topic.name().equals(topic))) {
				adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)));
				System.out.println("Topic '" + topic + "' created successfully.");
				return true;
			}
		} catch (Exception e) {
			System.err.println("Error checking or creating topic '" + topic + "': " + e.getMessage());
		}
		return false;
	}

	private void sendMessage() {

		try {
			for (int i = 0; i < 10; i++) {
				ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key__" + i, "value__" + i);
				producer.send(record).get();
			}
			System.out.println("10 Messages sent to topic '" + topic);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}

}
