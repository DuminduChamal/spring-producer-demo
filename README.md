# Kafka Producer with Spring Boot — Learning Project

A Spring Boot take on producing to Kafka with `KafkaTemplate`, after
building [`producer-demo`](https://github.com/DuminduChamal/producer-demo)
by hand with the plain `kafka-clients` API. Companion to
[`spring-consumer-demo`](https://github.com/DuminduChamal/spring-consumer-demo)
— same framework, opposite direction.

## Prerequisites

- **Java 17+** — Spring Boot 4.x's minimum supported version (also required
  by the Kafka 4.x broker itself).
- **Maven**
- **A local Kafka 4.3.1 broker**, running in KRaft mode at `localhost:9092`,
  with `keyed-topic` already created (see
  [`producer-demo`](https://github.com/DuminduChamal/producer-demo) for
  broker setup and topic creation).

## The Spring Boot 4 dependency gotcha (again)

Same issue hit in `spring-consumer-demo`: this project depends on
`org.springframework.boot:spring-boot-starter-kafka`, **not**
`org.springframework.kafka:spring-kafka` directly. As of Spring Boot 4,
`KafkaAutoConfiguration` lives only inside the starter — adding just
`spring-kafka` builds and starts cleanly with no error, but no
`KafkaTemplate` bean ever gets created.

## Configuration

`src/main/resources/application.properties`:

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
```

This file was originally created as `appilication.properties` (typo) and
Spring Boot silently ignored it — no error, just fell back to defaults.
It "worked" anyway only by coincidence, since Boot's own default
`spring.kafka.bootstrap-servers` already happens to be `localhost:9092`.
Worth knowing generally: **Spring Boot doesn't warn about an unrecognized
properties filename** — a name that isn't exactly `application.properties`
(or `application.yml`) is just never read, with no indication anything is
wrong. Fixed here by renaming the file.

## Why this project needs no `while (true)` or listener container

Unlike a consumer, a producer doesn't wait for anything — it sends in
response to *your application* deciding to, whenever that happens to be.
`producer-demo`'s examples are all one-shot (send, then exit via
try-with-resources); this project demonstrates both ends of that spectrum
with Spring:

### 1. `SimpleTemplateProducer` — one-shot, at startup
A `CommandLineRunner` bean, which Spring Boot calls once after the
application context finishes initializing. Sends one message via the
auto-configured `KafkaTemplate<String, String>` bean (built from
`application.properties` — no `Properties` object or `KafkaProducer`
constructed by hand, unlike every plain-client producer). Since nothing
else in a `spring-boot-starter`-only app creates a non-daemon thread, the
JVM exits right after this runs — same underlying mechanism as the very
first `spring-consumer-demo` gotcha (no listener container → no reason to
stay alive), just intentional here instead of a bug.

### 2. `MessageController` — long-running, on demand
```java
@PostMapping("/messages")
public ResponseEntity<String> sendMessage(@RequestBody MessageRequest request) throws Exception {
    SendResult<String, String> result = kafkaTemplate
            .send("keyed-topic", request.key(), request.value())
            .get();
    return ResponseEntity.ok(String.format("Sent via API -> partition=%d offset=%d",
            result.getRecordMetadata().partition(), result.getRecordMetadata().offset()));
}
```
This is what a real "always-on producer" actually looks like: not a loop
that sends on its own, but a `KafkaTemplate` sitting ready inside an
application that stays alive for an unrelated reason — here,
`spring-boot-starter-web`'s embedded Tomcat server, which runs on
non-daemon threads and is what actually keeps the JVM up this time.
`send()` only fires when an HTTP request triggers it.

**Where the result actually shows up**: the send result is only returned in
the HTTP response — there's no server-side console output per request, so
don't expect to see anything printed in the `mvn spring-boot:run` terminal
for each call (unlike `SimpleTemplateProducer`, which does print, since
it's a one-time startup action rather than a per-request handler).

## Running it

```bash
mvn compile
mvn spring-boot:run
```

(two-step form — same `exec`/build-plugin `ClassNotFoundException` risk
flagged in the other Spring project applies here too when something's just
changed)

Watch for `SimpleTemplateProducer`'s startup send to print immediately,
followed by Tomcat starting on port 8080 — and this time **the app stays
running**, unlike a plain `CommandLineRunner`-only app would.

Send a message via the REST endpoint:

```bash
curl -X POST http://localhost:8080/messages \
  -H "Content-Type: application/json" \
  -d '{"key": "http-key", "value": "sent via REST"}'
```

Verify either with the console consumer, or with any already-running
listener from `consumer-demo`/`spring-consumer-demo` subscribed to
`keyed-topic`:

```bash
cd ~/Documents/Learnings/Kafka/kafka
bin/kafka-console-consumer.sh --topic keyed-topic --from-beginning \
  --bootstrap-server localhost:9092 --property print.key=true --property key.separator=":"
```

## What's next

Avro + Schema Registry support (`AvroMessageController`, mirroring
`spring-consumer-demo`'s `AvroOrderEventListener`) — needs its own
dedicated `ProducerFactory`/`KafkaTemplate<String, OrderEventAvro>` bean
pair, since (unlike `@KafkaListener`'s per-listener `properties` override)
`KafkaTemplate` has no per-send configuration override mechanism.
