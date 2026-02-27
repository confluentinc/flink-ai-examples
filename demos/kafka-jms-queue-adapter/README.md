# Kafka JMS Queue Adapter

A **JMS 2.0 Queue API** on top of Apache Kafka using [Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) (KIP-932) share groups. Use standard JMS queue semantics (`ConnectionFactory`, `JMSContext`, `Queue`, `TextMessage`) with Kafka as the backend — similar to how [Amazon SQS Java Messaging Library](https://github.com/awslabs/amazon-sqs-java-messaging-lib) provides JMS for SQS.

## Add to Your Project

Add the dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-jms-queue-adapter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Then install the library locally (or publish to your Maven repository):

```bash
cd kafka-jms-queue-adapter   # or your project directory
mvn clean install
```

## Quick Start

```java
import io.confluent.jms.KafkaConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Queue;

// Create connection factory (analogous to SQSConnectionFactory)
KafkaConnectionFactory factory = KafkaConnectionFactory.create("localhost:9092");

try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
    Queue queue = ctx.createQueue("orders-queue");
    ctx.createProducer().send(queue, "Hello");
    var consumer = ctx.createConsumer(queue);
    var msg = consumer.receive(5000);
    if (msg != null) {
        System.out.println("Received: " + ((javax.jms.TextMessage) msg).getText());
    }
}
```

Async consumption with `MessageListener`:

```java
consumer.setMessageListener(msg -> {
    try {
        System.out.println("Received: " + ((TextMessage) msg).getText());
    } catch (JMSException e) { /* handle */ }
});
// Listener runs in background; keep context/consumer open
```

## Scope

- **Queues only** — Topics, durable subscriptions, and shared consumers are not supported
- **TextMessage only** — BytesMessage, MapMessage, ObjectMessage, StreamMessage throw `UnsupportedOperationException`
- **Point-to-point** — Each message is delivered to exactly one consumer (Kafka share groups)
- **Session modes** — `AUTO_ACKNOWLEDGE` and `CLIENT_ACKNOWLEDGE` supported
- **Async consumption** — `setMessageListener()` for push-style delivery (cannot be combined with `receive()`)

## Project Structure

```
kafka-jms-queue-adapter/
├── pom.xml                    # Root POM
├── kafka-jms-queue-adapter/   # Library module
└── jms-adapter-examples/      # Example clients
```

| Module | Description |
|--------|-------------|
| `kafka-jms-queue-adapter` | Library JAR — JMS implementation only |
| `jms-adapter-examples` | Example clients (send/receive, simulation, listener) |

## Build

From the project root:

```bash
mvn clean install
```

This produces (in `kafka-jms-queue-adapter/target/`):
- `kafka-jms-queue-adapter-1.0-SNAPSHOT.jar` — main library
- `kafka-jms-queue-adapter-1.0-SNAPSHOT-sources.jar` — source
- `kafka-jms-queue-adapter-1.0-SNAPSHOT-javadoc.jar` — Javadoc

## Run Examples

From the project root:

```bash
cd jms-adapter-examples

# Simple send/receive
mvn exec:java -Pclient

# Simulate multiple producers and consumers
mvn exec:java -Psimulate

# Async consumption with MessageListener
mvn exec:java -Plistener
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `QUEUE_NAME` | `orders-queue` | Queue (topic) name |
| `PRODUCERS` | `3` | Number of producer threads (simulate profile only) |
| `CONSUMERS` | `3` | Number of consumer threads (simulate profile only) |
| `MESSAGES_PER_PRODUCER` | `5` | Messages per producer (simulate profile only) |
| `MESSAGES` | `5` | Number of messages to send (listener profile only) |

## Prerequisites

- Java 17+
- Kafka 4.2+ with share groups enabled

For local Kafka:

```bash
docker run --name kafka_qfk --rm -p 9092:9092 apache/kafka:4.2.0
docker exec kafka_qfk sh -c "
  /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 upgrade --feature share.version=1 &&
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-queue --partitions 1
"
```

## API Summary

| JMS API | Supported |
|---------|-----------|
| `ConnectionFactory.createContext()` | ✅ |
| `JMSContext.createQueue()` | ✅ |
| `JMSContext.createProducer()` | ✅ |
| `JMSContext.createConsumer(Queue)` | ✅ |
| `JMSProducer.send(Queue, String)` | ✅ |
| `JMSConsumer.receive()` / `receive(timeout)` | ✅ |
| `JMSConsumer.setMessageListener()` (async) | ✅ |
| `TextMessage` / `receiveBody(Class)` | ✅ |
| `AUTO_ACKNOWLEDGE` / `CLIENT_ACKNOWLEDGE` | ✅ |
| JMS headers: `JMSMessageID`, `JMSDestination`, `JMSTimestamp` | ✅ (on receive) |
| Topics, BytesMessage, MapMessage, etc. | ❌ UnsupportedOperationException |

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Bootstrap servers | `localhost:9092` | Pass to `KafkaConnectionFactory.create(bootstrap)` |
| Share group prefix | `jms-adapter-share-group` | Configurable via `KafkaConnectionFactory.create(bootstrap, prefix)` |
| `CONFLUENT_API_KEY` | — | Confluent Cloud API key (env var, enables SASL_SSL) |
| `CONFLUENT_API_SECRET` | — | Confluent Cloud API secret (env var) |

**Session modes:** `AUTO_ACKNOWLEDGE` and `CLIENT_ACKNOWLEDGE` are supported. Use `message.acknowledge()` for explicit ack in `CLIENT_ACKNOWLEDGE` mode.

## Troubleshooting

**"No message received"** — Ensure:
1. **Share groups enabled**: `kafka-features.sh upgrade --feature share.version=1`
2. **Topic exists**: `kafka-topics.sh --create --topic orders-queue --partitions 1`
3. **Correct queue name**: Confluent Cloud Terraform may create `orders-topic`; use `QUEUE_NAME=orders-topic` if so
4. **Confluent Cloud**: Set `CONFLUENT_API_KEY` and `CONFLUENT_API_SECRET` for SASL authentication
