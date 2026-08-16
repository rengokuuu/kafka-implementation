package com.demo.kafka.producer.service;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.demo.kafka.model.OrderEvent;

@Service
public class OrderProducerService {
	
	private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);
	
	private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
	private final String topic;
	public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate, @Value("${app.kafka.topic.orders}") String topic) {
		this.kafkaTemplate = kafkaTemplate;
		this.topic = topic;
	}
	
	public CompletableFuture<SendResult<String, OrderEvent>> publish(OrderEvent event){
		CompletableFuture<SendResult<String, OrderEvent>> future = kafkaTemplate.send(topic, event.customerId(), event);
		
		future.whenComplete((result, ex)->{
			if(ex != null) {
				log.error("Failed to public order event orderId={}, customerId={}", event.orderId(), event.customerId(), ex);
			}
			else {
				log.info("Published order event orderId={}, partition={}, offset={}",
						event.orderId(),
						result.getRecordMetadata().partition(),
						result.getRecordMetadata().offset());
			}
		});
				return future;
	}

}
