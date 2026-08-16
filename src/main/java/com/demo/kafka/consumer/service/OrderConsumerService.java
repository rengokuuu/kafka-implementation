package com.demo.kafka.consumer.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import com.demo.kafka.model.OrderEvent;

@Service
public class OrderConsumerService {
	
	private static final Logger log = LoggerFactory.getLogger(OrderConsumerService.class);
	
	@KafkaListener(topics = "${app.kafka.topic.orders}", groupId = "${app.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
	public void consumer(ConsumerRecord<String, OrderEvent> record, Acknowledgment ack) {
		OrderEvent event = record.value();
		try {
			log.info("Consume order event orderId={} customerId={} partition={} offset={}", event.orderId(), event.customerId(), record.partition(), record.offset());
			processOrder(event);
		}
		catch(Exception e) {
			log.error(""+e.getMessage());
			throw e;
		}
	}
	
	private void processOrder(OrderEvent event) {
		if(event.amount() == null || event.amount().signum()<=0) {
			throw new IllegalArgumentException("Invalid Order Amount for OrderId="+event.orderId());
		}
	}
}
