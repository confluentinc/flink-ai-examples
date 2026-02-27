package io.confluent.jms;

import javax.jms.JMSContext;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simulates multiple producers and consumers on a JMS queue backed by Kafka share groups.
 * Each message is delivered to exactly one consumer (queue semantics).
 *
 * Usage: mvn exec:java -Psimulate
 * Env: PRODUCERS=3, CONSUMERS=3, MESSAGES_PER_PRODUCER=5, QUEUE_NAME=orders-queue
 */
public class JmsQueueSimulation {

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String queueName = System.getenv().getOrDefault("QUEUE_NAME", "orders-queue");
        int numProducers = Integer.parseInt(System.getenv().getOrDefault("PRODUCERS", "3"));
        int numConsumers = Integer.parseInt(System.getenv().getOrDefault("CONSUMERS", "3"));
        int messagesPerProducer = Integer.parseInt(System.getenv().getOrDefault("MESSAGES_PER_PRODUCER", "5"));

        KafkaConnectionFactory factory = KafkaConnectionFactory.create(bootstrap);
        CountDownLatch consumersReady = new CountDownLatch(numConsumers);
        CountDownLatch producersDone = new CountDownLatch(numProducers);
        AtomicInteger totalReceived = new AtomicInteger(0);
        int totalToSend = numProducers * messagesPerProducer;

        System.out.println("=== JMS Queue Simulation ===");
        System.out.println("Producers: " + numProducers + ", Consumers: " + numConsumers);
        System.out.println("Messages: " + totalToSend + " (" + messagesPerProducer + " per producer)");
        System.out.println("Queue: " + queueName);
        System.out.println();

        // Start consumers first so they can join the share group
        Thread[] consumerThreads = new Thread[numConsumers];
        for (int c = 0; c < numConsumers; c++) {
            final int consumerId = c + 1;
            consumerThreads[c] = new Thread(() -> {
                try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
                     var consumer = ctx.createConsumer(ctx.createQueue(queueName))) {
                    consumersReady.countDown();
                    while (totalReceived.get() < totalToSend) {
                        TextMessage msg = (TextMessage) consumer.receive(3000);
                        if (msg != null) {
                            try {
                                String body = msg.getText();
                                int received = totalReceived.incrementAndGet();
                                System.out.println("  Consumer-" + consumerId + " received [" + received + "/" + totalToSend + "]: " + body);
                            } catch (Exception e) {
                                System.err.println("  Consumer-" + consumerId + " error: " + e.getMessage());
                            }
                        }
                    }
                }
            });
            consumerThreads[c].start();
        }

        // Wait for consumers to be ready, then allow share group join
        consumersReady.await(15, TimeUnit.SECONDS);
        System.out.println("Consumers ready. Waiting 4s for share group join...\n");
        Thread.sleep(4000);

        // Start producers
        Thread[] producerThreads = new Thread[numProducers];
        for (int p = 0; p < numProducers; p++) {
            final int producerId = p + 1;
            producerThreads[p] = new Thread(() -> {
                try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
                    Queue queue = ctx.createQueue(queueName);
                    for (int i = 0; i < messagesPerProducer; i++) {
                        String payload = "P" + producerId + "-msg" + (i + 1);
                        ctx.createProducer().send(queue, payload);
                        System.out.println("Producer-" + producerId + " sent: " + payload);
                        try { Thread.sleep(100); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
                    }
                }
                producersDone.countDown();
            });
            producerThreads[p].start();
        }

        producersDone.await(30, TimeUnit.SECONDS);
        for (Thread t : consumerThreads) {
            t.join(5000);
        }

        System.out.println("\nDone. Received " + totalReceived.get() + "/" + totalToSend + " messages.");
    }
}
