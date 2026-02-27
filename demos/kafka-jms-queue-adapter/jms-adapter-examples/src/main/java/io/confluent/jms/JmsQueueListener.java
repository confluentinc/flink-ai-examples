package io.confluent.jms;

import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.TextMessage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Example demonstrating async consumption with MessageListener.
 * Sets a listener, sends messages, and the listener receives them in the background.
 *
 * Usage: mvn exec:java -Plistener
 */
public class JmsQueueListener {

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String queueName = System.getenv().getOrDefault("QUEUE_NAME", "orders-queue");
        int numMessages = Integer.parseInt(System.getenv().getOrDefault("MESSAGES", "5"));

        KafkaConnectionFactory factory = KafkaConnectionFactory.create(bootstrap);
        CountDownLatch listenerReady = new CountDownLatch(1);
        CountDownLatch messagesReceived = new CountDownLatch(numMessages);
        AtomicInteger receivedCount = new AtomicInteger(0);

        // Consumer with MessageListener - runs in background thread
        Thread consumerThread = new Thread(() -> {
            try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
                 var consumer = ctx.createConsumer(ctx.createQueue(queueName))) {

                MessageListener listener = msg -> {
                    try {
                        String text = ((TextMessage) msg).getText();
                        int n = receivedCount.incrementAndGet();
                        System.out.println("  Listener received [" + n + "/" + numMessages + "]: " + text);
                        messagesReceived.countDown();
                    } catch (JMSException e) {
                        System.err.println("  Listener error: " + e.getMessage());
                    }
                };

                consumer.setMessageListener(listener);
                listenerReady.countDown();

                // Keep context open until all messages received or timeout
                messagesReceived.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        consumerThread.start();

        // Wait for listener to be ready, then allow share group join
        listenerReady.await(10, TimeUnit.SECONDS);
        System.out.println("Listener ready. Waiting 4s for share group join...\n");
        Thread.sleep(4000);

        // Send messages
        try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
            Queue queue = ctx.createQueue(queueName);
            for (int i = 1; i <= numMessages; i++) {
                String payload = "msg-" + i + " at " + System.currentTimeMillis();
                ctx.createProducer().send(queue, payload);
                System.out.println("Sent: " + payload);
                Thread.sleep(300);
            }
        }

        // Allow listener time to receive last message
        Thread.sleep(2000);
        consumerThread.join(65000);
        System.out.println("\nDone. Listener received " + receivedCount.get() + "/" + numMessages + " messages.");
    }
}
