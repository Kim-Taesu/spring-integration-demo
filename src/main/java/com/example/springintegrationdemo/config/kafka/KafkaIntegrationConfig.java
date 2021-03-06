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
	 * Kafka Producer ??????
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
	 * Kafka Consumer ??????
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
	 * Kafka Message Listener ??????
	 * - Single-threaded Message listener container using the Java Consumer supporting auto-partition assignment or user-configured assignment.
	 * - application.yml ?????? ????????? topic??? ????????????.
	 */
	@Bean
	public KafkaMessageListenerContainer<String, String> container(ConsumerFactory<String, String> kafkaConsumerFactory) {
		return new KafkaMessageListenerContainer<>(kafkaConsumerFactory, new ContainerProperties(new TopicPartitionOffset(this.properties.getTopic(), 0)));
	}

	/**
	 * ?????? ????????? : Kafka ??? ????????? ???????????? Kafka?????? consume ??? ???????????? ??????
	 * - ?????? ???????????? ?????? ???????????? ????????? ????????? ????????????.
	 * - ???????????? Inbound ?????? ???????????? ?????? Integration Flow ??? ???????????? Outbound ?????? ???????????? ?????? Integration Flow ?????? ?????????.
	 *
	 * ?????? ??????????????? kafka ?????? consume ??? ???????????? fromKafka(outbound ?????? ?????????)??? ???????????? ?????????.
	 *
	 * @param container kafka listener container
	 */
	@Bean
	public KafkaMessageDrivenChannelAdapter<String, String> adapter(KafkaMessageListenerContainer<String, String> container) {
		KafkaMessageDrivenChannelAdapter<String, String> kafkaMessageDrivenChannelAdapter = new KafkaMessageDrivenChannelAdapter<>(container);
		kafkaMessageDrivenChannelAdapter.setOutputChannel(fromKafka()); // output channel ??????
		return kafkaMessageDrivenChannelAdapter;
	}

	@Bean
	public PollableChannel fromKafka() {
		return new QueueChannel();
	}

	/**
	 * Target System(Kafka)??? ????????? outbound channel adapter ??????
	 * 	- Kafka ???????????? ???????????? ????????? ?????? "toKafka" inputChannel ??? ????????????.
	 * 	- "toKafka" ?????? MessageChannel??? Bean?????? ?????????
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
	 * ??????????????? ?????? ?????? ???????????? ???????????? ?????? consumer group ??? ????????????.
	 * - IntegrationFlow??? ???????????? kafka??? ???????????? fromKafka ????????? ?????????.
	 *
	 * @param topics ???????????? ?????? ????????? ?????? ?????????
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

