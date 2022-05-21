package com.example.springintegrationdemo.service.kafka;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Service;

import com.example.springintegrationdemo.config.kafka.KafkaAppProperties;
import com.example.springintegrationdemo.config.kafka.KafkaIntegrationConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaIntegrationService {

	private final KafkaAppProperties kafkaAppProperties;
	private final KafkaIntegrationConfig kafkaIntegrationConfig;

	public void runDemo(ConfigurableApplicationContext context) {

		// toKafka 메시지 채널을 사용하여 메시지 전송
		MessageChannel toKafka = context.getBean("toKafka", MessageChannel.class);
		log.info("Sending 10 messages...");
		Map<String, Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, this.kafkaAppProperties.getTopic());
		for (int i = 0; i < 10; i++) {
			GenericMessage<String> message;
			if (i % 2 == 0) {
				message = new GenericMessage<>("even" + i, headers);
			} else {
				message = new GenericMessage<>("odd" + i, headers);
			}
			log.info("Send Message: {}", message);
			toKafka.send(message);
		}
		log.info("Sending a null message...");
		toKafka.send(new GenericMessage<>(KafkaNull.INSTANCE, headers));

		// fromKafka 메시지 채널을 사용하여 메시지 수신
		PollableChannel fromKafka = context.getBean("fromKafkaByTransformer", PollableChannel.class);
		Message<?> received = fromKafka.receive(10000);
		int count = 0;
		while (received != null) {
			log.info("Receive Message: {}", received);
			received = fromKafka.receive(++count < 11 ? 10000 : 1000);
		}

		log.info("Adding an adapter for a second topic and sending 10 messages...");
		kafkaIntegrationConfig.addAnotherListenerForTopics(this.kafkaAppProperties.getNewTopic());
		headers = Collections.singletonMap(KafkaHeaders.TOPIC, this.kafkaAppProperties.getNewTopic());
		for (int i = 0; i < 10; i++) {
			toKafka.send(new GenericMessage<>("bar" + i, headers));
		}

		received = fromKafka.receive(10000);
		count = 0;
		while (received != null) {
			log.info(received.toString());
			received = fromKafka.receive(++count < 10 ? 10000 : 1000);
		}
	}

}
