package com.example.springintegrationdemo;

import java.util.Collections;
import java.util.Map;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import com.example.springintegrationdemo.config.KafkaAppProperties;
import com.example.springintegrationdemo.config.KafkaIntegrationConfig;
import lombok.extern.slf4j.Slf4j;

@EnableIntegration
@SpringBootApplication
@EnableConfigurationProperties
@Slf4j
public class SpringIntegrationDemoApplication {

	private final KafkaAppProperties kafkaAppProperties;
	private final KafkaIntegrationConfig kafkaIntegrationConfig;

	public SpringIntegrationDemoApplication(KafkaAppProperties kafkaAppProperties, KafkaIntegrationConfig kafkaIntegrationConfig) {
		this.kafkaAppProperties = kafkaAppProperties;
		this.kafkaIntegrationConfig = kafkaIntegrationConfig;
	}

	public static void main(String[] args) {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(SpringIntegrationDemoApplication.class)
			.web(WebApplicationType.NONE)
			.run(args);

		context.getBean(SpringIntegrationDemoApplication.class).runDemo(context);
		context.close();
	}

	private void runDemo(ConfigurableApplicationContext context) {

		// toKafka 메시지 채널을 사용하여 메시지 전송
		MessageChannel toKafka = context.getBean("toKafka", MessageChannel.class);
		log.info("Sending 10 messages...");
		Map<String, Object> headers = Collections.singletonMap(KafkaHeaders.TOPIC, this.kafkaAppProperties.getTopic());
		for (int i = 0; i < 10; i++) {
			toKafka.send(new GenericMessage<>("foo" + i, headers));
		}
		log.info("Sending a null message...");
		toKafka.send(new GenericMessage<>(KafkaNull.INSTANCE, headers));

		// fromKafka 메시지 채널을 사용하여 메시지 수신
		PollableChannel fromKafka = context.getBean("fromKafka", PollableChannel.class);
		Message<?> received = fromKafka.receive(10000);
		int count = 0;
		while (received != null) {
			log.info(received.toString());
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

	/**
	 * Kafka 토픽으로 메시지를 보내기 위해 "toKafka" inputChannel 을 사용한다.
	 * 	- "toKafka" 라는 MessageChannel이 Bean으로 생성됨
	 *
	 * @param kafkaTemplate kafkaTempalte
	 */
	@Bean
	@ServiceActivator(inputChannel = "toKafka")
	public MessageHandler handler(KafkaTemplate<String, String> kafkaTemplate) {
		KafkaProducerMessageHandler<String, String> handler = new KafkaProducerMessageHandler<>(kafkaTemplate);
		handler.setMessageKeyExpression(new LiteralExpression(this.kafkaAppProperties.getMessageKey()));
		return handler;
	}
}
