package com.learning.kafka;

public record OrderEventRequest(String orderId, double amount, String customerId) {}