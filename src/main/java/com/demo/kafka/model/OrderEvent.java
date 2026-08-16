package com.demo.kafka.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Builder;

@Builder
public record OrderEvent(
		String orderId,
		String customerId,
		BigDecimal amount,
		String status,
		Instant createdAt
		) implements Serializable{
	
	@JsonCreator
	public OrderEvent(@JsonProperty("orderId") String orderId,
			@JsonProperty("customerId") String customerId,
			@JsonProperty("amount") BigDecimal amount,
			@JsonProperty("status") String status,
			@JsonProperty("createdAt") Instant createdAt) {
		this.orderId = orderId;
		this.customerId = customerId;
		this.amount = amount;
		this.status = status;
		this.createdAt = createdAt;
	}
}
