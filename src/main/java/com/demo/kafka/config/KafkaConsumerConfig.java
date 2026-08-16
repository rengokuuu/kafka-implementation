package com.demo.kafka.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import com.demo.kafka.model.OrderEvent;

@Configuration
public class KafkaConsumerConfig {
	
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;
	
	@Value("${app.kafka.consumer.group-id}")
	private String groupId;
	
	@Value("${app.kafka.consumer.concurrency:3}")
	private int concurrency;
	
	@Bean
	public ConsumerFactory<String, OrderEvent> consumerFactory(){
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		config.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
		config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.demo.kafka.model");
		config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
		config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300_000);
		config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);
		
		return new DefaultKafkaConsumerFactory<>(config);
	}
	
	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaListenerContainerFactory(
			ConsumerFactory<String, OrderEvent> consumerFactory,
			KafkaTemplate<String, OrderEvent> kafkaTemplate){
		ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
		
		factory.setConsumerFactory(consumerFactory);
		factory.setConcurrency(concurrency);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		factory.setCommonErrorHandler(errorHandler(kafkaTemplate));
		return factory;
	}
	
	@Bean
	public DefaultErrorHandler errorHandler(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
		
		ExponentialBackOff backoff = new ExponentialBackOff();
		backoff.setInitialInterval(500L);
		backoff.setMultiplier(2.0);
		backoff.setMaxInterval(10_000L);
		backoff.setMaxElapsedTime(Duration.ofSeconds(30).toMillis());
		
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backoff);
		errorHandler.addNotRetryableExceptions(SerializationException.class);
		return errorHandler;
		
	}
	
}
