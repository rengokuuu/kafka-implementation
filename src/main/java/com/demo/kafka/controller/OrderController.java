package com.demo.kafka.controller;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.demo.kafka.dto.CreateOrderRequest;
import com.demo.kafka.model.OrderEvent;
import com.demo.kafka.producer.service.OrderProducerService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class OrderController {
	
	private final OrderProducerService orderProducerService;
	
	@PostMapping(value = "/api/orders")
	public ResponseEntity<Map<String, String>> createOrder(@Valid @RequestBody CreateOrderRequest request){
		String orderId = UUID.randomUUID().toString();
		OrderEvent event = OrderEvent.builder()
				.orderId(orderId)
				.customerId(request.customerId())
				.amount(request.amount())
				.status("CREATED")
				.createdAt(Instant.now())
				.build();
		
		orderProducerService.publish(event);
		return ResponseEntity.accepted().body(Map.of(
				"orderId", orderId,
				"status", "ACCEPTED"
				));
	}
}
