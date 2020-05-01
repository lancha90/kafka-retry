package com.dmha.lab.kafkaretry;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.retry.annotation.EnableRetry;

@EnableRetry
@SpringBootApplication
public class KafkaRetryApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaRetryApplication.class, args);
	}

}
