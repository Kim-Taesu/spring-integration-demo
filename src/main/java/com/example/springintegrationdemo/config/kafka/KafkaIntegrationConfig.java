package com.example.springintegrationdemo.config.kafka;

import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.expression.common.LiteralExpression;
import org.springframework.integration.annotation.Filter;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.context.IntegrationFlowContext;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.outbound.KafkaProducerMessageHandler;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.PollableChannel;
import org.springframework.messaging.support.GenericMessage;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
public class KafkaIntegrationConfig {

	private final KafkaAppProperties properties;
	private final IntegrationFlowContext flowContext;
	private final KafkaProperties kafkaProperties;

	/**
	 * Kafka Producer 설정
	 *
	 * @param properties kafka properties
	 */
	@Bean
	public ProducerFactory<?, ?> kafkaProducerFactory(KafkaProperties properties) {
		Map<String, Object> producerProperties = properties.buildProducerProperties();
		producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		return new DefaultKafkaProducerFactory<>(producerProperties);
	}

	/**
	 * Kafka Consumer 설정
	 *
	 * @param properties kafka properties
	 */
	@Bean
	public ConsumerFactory<?, ?> kafkaConsumerFactory(KafkaProperties properties) {
		Map<String, Object> consumerProperties = properties.buildConsumerProperties();
		consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
		return new DefaultKafkaConsumerFactory<>(consumerProperties);
	}

	/**
	 * Kafka Message Listener 설정
	 * - Single-threaded Message listener container using the Java Consumer supporting auto-partition assignment or user-configured assignment.
	 * - application.yml 에서 설정한 topic을 바라본다.
	 */
	@Bean
	public KafkaMessageListenerContainer<String, String> container(ConsumerFactory<String, String> kafkaConsumerFactory) {
		return new KafkaMessageListenerContainer<>(kafkaConsumerFactory, new ContainerProperties(new TopicPartitionOffset(this.properties.getTopic(), 0)));
	}

	/**
	 * 채널 어댑터 : Kafka 에 채널을 연결하여 Kafka에서 consume 한 메시지를 받음
	 * - 채널 어댑터는 통합 플로우의 입구와 출구를 나타낸다.
	 * - 데이터는 Inbound 채널 어댑터를 통해 Integration Flow 로 들어오고 Outbound 채널 어댑터를 통해 Integration Flow 에서 나간다.
	 *
	 * 아래 예제에서는 kafka 에서 consume 한 메시지를 fromKafka(outbound 채널 어댑터)로 메시지를 보낸다.
	 *
	 * @param container kafka listener container
	 */
	@Bean
	public KafkaMessageDrivenChannelAdapter<String, String> adapter(KafkaMessageListenerContainer<String, String> container) {
		KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>(container);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(fromKafka()); // output channel 설정
		return kafkaMessageDrivenChannelAdapter;
	}

	@Bean
	public PollableChannel fromKafka() {
		return new QueueChannel();
	}

	/**
	 * Target System(Kafka)에 연결된 outbound channel adapter 설정
	 * 	- Kafka 토픽으로 메시지를 보내기 위해 "toKafka" inputChannel 을 사용한다.
	 * 	- "toKafka" 라는 MessageChannel이 Bean으로 생성됨
	 *
	 * @param kafkaTemplate kafkaTempalte
	 */
	@Bean
	@ServiceActivator(inputChannel = "toKafka")
	public MessageHandler handler(KafkaTemplate<String, String> kafkaTemplate) {
		KafkaProducerMessageHandler<String, String> handler = new KafkaProducerMessageHandler<>(kafkaTemplate);
		handler.setMessageKeyExpression(new LiteralExpression(this.properties.getMessageKey()));
		return handler;
	}

	@Bean
	public NewTopic topic(KafkaAppProperties properties) {
		return new NewTopic(properties.getTopic(), 1, (short)1);
	}

	@Bean
	public NewTopic newTopic(KafkaAppProperties properties) {
		return new NewTopic(properties.getNewTopic(), 1, (short)1);
	}

	/**
	 * 파라미터로 받은 토픽 메시지를 수신하는 다른 consumer group 을 추가한다.
	 * - IntegrationFlow를 생성하고 kafka의 메시지를 fromKafka 채널로 받는다.
	 *
	 * @param topics 메시지를 받을 카프카 토픽 리스트
	 */
	public void addAnotherListenerForTopics(String... topics) {
		Map<String, Object> consumerProperties = kafkaProperties.buildConsumerProperties();
		// change the group id so we don't revoke the other partitions.
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG) + "x");
		IntegrationFlow flow = IntegrationFlows.from(Kafka.messageDrivenChannelAdapter(new DefaultKafkaConsumerFactory<String, String>(consumerProperties), topics))
			.channel("fromKafka") // outputChannel
			.get();
		this.flowContext.registration(flow).register();
	}

	@Filter(inputChannel = "fromKafka", outputChannel = "fromKafkaByFilter")
	public boolean kafkaFilter(GenericMessage<String> message) {
		return message.getPayload().contains("odd");
	}

	@Bean
	public PollableChannel fromKafkaByFilter() {
		return new QueueChannel();
	}

	@Transformer(inputChannel = "fromKafkaByFilter", outputChannel = "fromKafkaByTransformer")
	public String kafkaTransformer(GenericMessage<String> message) {
		return message.getPayload().toUpperCase();
	}

	@Bean
	public PollableChannel fromKafkaByTransformer() {
		return new QueueChannel();
	}

}

