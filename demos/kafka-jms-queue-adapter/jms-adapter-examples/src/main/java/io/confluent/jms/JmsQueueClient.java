package io.confluent.jms;

import javax.jms.JMSContext;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Example JMS client for the Kafka-backed queue adapter.
 * Starts a receiver (separate context), then sends a message for it to receive.
 *
 * Usage: mvn exec:java
 */
public class JmsQueueClient {

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String queueName = System.getenv().getOrDefault("QUEUE_NAME", "orders-queue");

        KafkaConnectionFactory factory = KafkaConnectionFactory.create(bootstrap);
        AtomicReference<TextMessage> received = new AtomicReference<>();
        CountDownLatch receiverReady = new CountDownLatch(1);

        // Receiver in separate context/thread - joins share group before we send
        Thread receiver = new Thread(() -> {
            try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
                 var consumer = ctx.createConsumer(ctx.createQueue(queueName))) {
                receiverReady.countDown();
                TextMessage msg = (TextMessage) consumer.receive(15000);
                received.set(msg);
            }
        });
        receiver.start();

        // Wait for receiver to subscribe, then allow share group join
        receiverReady.await(10, TimeUnit.SECONDS);
        Thread.sleep(4000);  // KafkaShareConsumer needs time to join share group

        // Send
        try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
            Queue queue = ctx.createQueue(queueName);
            String payload = "Hello from JMS at " + System.currentTimeMillis();
            ctx.createProducer().send(queue, payload);
            System.out.println("Sent: " + payload);
        }

        receiver.join(20000);
        TextMessage msg = received.get();
        if (msg != null) {
            try {
                System.out.println("Received: " + msg.getText());
            } catch (javax.jms.JMSException e) {
                System.err.println("Error reading message: " + e.getMessage());
            }
        } else {
            System.out.println("No message received. Ensure: 1) Kafka has share groups enabled, 2) Topic exists.");
        }
        System.out.println("Done.");
    }
}
