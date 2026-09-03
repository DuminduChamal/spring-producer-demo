package com.learning.kafka.dto;

public record OrderEventRequest(String orderId, double amount, String customerId) {}