package com.example.springintegrationdemo.config.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Configuration
@ConfigurationProperties("kafka")
public class KafkaAppProperties {
	private String topic;
	private String newTopic;
	private String messageKey;
}
