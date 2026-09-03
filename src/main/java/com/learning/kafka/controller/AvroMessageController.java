package com.learning.kafka.controller;

import com.learning.kafka.OrderEventAvro;
import com.learning.kafka.dto.OrderEventRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AvroMessageController {

    private final KafkaTemplate<String, OrderEventAvro> avroKafkaTemplate;

    public AvroMessageController(KafkaTemplate<String, OrderEventAvro> avroKafkaTemplate) {
        this.avroKafkaTemplate = avroKafkaTemplate;
    }

    @PostMapping("/avro-messages")
    public ResponseEntity<String> sendAvroMessage(@RequestBody OrderEventRequest request) throws Exception {
        OrderEventAvro order = OrderEventAvro.newBuilder()
                .setOrderId(request.orderId())
                .setAmount(request.amount())
                .setTimestamp(System.currentTimeMillis())
                .setCustomerId(request.customerId())
                .build();

        SendResult<String, OrderEventAvro> result = avroKafkaTemplate
                .send("avro-orders-topic", order.getOrderId(), order)
                .get();

        return ResponseEntity.ok(String.format("Sent Avro -> partition=%d offset=%d",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
    }
}