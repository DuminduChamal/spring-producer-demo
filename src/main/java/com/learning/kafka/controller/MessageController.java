package com.learning.kafka.controller;

import com.learning.kafka.dto.MessageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

// This is what a real "long-running producer" looks like: the KafkaTemplate
// doesn't loop on its own, it just sits here ready, and send() only fires
// in response to an actual HTTP request arriving - same idea discussed for
// SimpleTemplateProducer, now made concrete.
@RestController
public class MessageController {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public MessageController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/messages")
    public ResponseEntity<String> sendMessage(@RequestBody MessageRequest request) throws Exception {
        SendResult<String, String> result = kafkaTemplate
                .send("keyed-topic", request.key(), request.value())
                .get();

        return ResponseEntity.ok(String.format("Sent via API -> partition=%d offset=%d",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
    }
}