package com.mehmood86.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoKafkaStreamsAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoKafkaStreamsAppApplication.class, args);
		System.out.println("Welcome");
	}

}
