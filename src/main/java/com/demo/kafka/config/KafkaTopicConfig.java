package com.demo.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
	
	@Value("${app.kafka.topic.orders}")
	private String ordersTopic;
	
	@Value("${app.kafka.topic.orders-dlt}")
	private String ordersDltTopic;
	
	@Value("${app.kafka.topic.partitions:3}")
	private int partitions;
	
	@Value("${app.kafka.topic.replication-factor:3}")
	private int replicationFactory;
	
	@Bean
	public NewTopic ordersTopic() {
		return TopicBuilder.name(ordersTopic).partitions(partitions).replicas(replicationFactory).build();
	}
	
	@Bean
	public NewTopic ordersDltTopic() {
		return TopicBuilder.name(ordersDltTopic).partitions(partitions).replicas(replicationFactory).build();
	}
}
