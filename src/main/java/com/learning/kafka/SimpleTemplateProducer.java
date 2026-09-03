package com.learning.kafka;

import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

// Baseline producer: Spring's KafkaTemplate instead of a hand-built
// KafkaProducer. Unlike a consumer's listener container (which runs
// forever on its own thread), sending is a one-off action, so this uses
// CommandLineRunner to trigger a send once at application startup rather
// than reacting to an incoming event.
@Component
public class SimpleTemplateProducer implements CommandLineRunner {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public SimpleTemplateProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        // send() is async, returning a CompletableFuture<SendResult<K, V>> -
        // the Spring equivalent of the plain client's Future<RecordMetadata>.
        // .get() blocks here just to see the result clearly, same reasoning
        // as SimpleProducer's .get() in producer-demo.
        SendResult<String, String> result = kafkaTemplate
                .send("keyed-topic", "spring-key", "Hello from Spring KafkaTemplate!")
                .get();

        System.out.printf("Sent -> partition=%d offset=%d%n",
                result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
    }
}