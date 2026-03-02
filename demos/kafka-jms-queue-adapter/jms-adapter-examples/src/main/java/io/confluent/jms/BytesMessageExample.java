package io.confluent.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSContext;
import javax.jms.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating BytesMessage support with primitive types.
 * Sends structured binary data (order information) and receives it.
 *
 * Usage: mvn exec:java -Pbytes
 */
public class BytesMessageExample {

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
        String queueName = System.getenv().getOrDefault("QUEUE_NAME", "orders-queue");

        KafkaConnectionFactory factory = KafkaConnectionFactory.create(bootstrap);
        CountDownLatch receiverReady = new CountDownLatch(1);
        CountDownLatch messageReceived = new CountDownLatch(1);

        System.out.println("=== JMS BytesMessage Example ===");
        System.out.println("Queue: " + queueName);
        System.out.println();

        // Receiver in separate thread - demonstrates reading primitives from BytesMessage
        Thread receiver = new Thread(() -> {
            try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
                 var consumer = ctx.createConsumer(ctx.createQueue(queueName))) {

                receiverReady.countDown();
                BytesMessage msg = (BytesMessage) consumer.receive(15000);

                if (msg != null) {
                    try {
                        // Read primitives in the same order they were written
                        int orderId = msg.readInt();
                        String productName = msg.readUTF();
                        double price = msg.readDouble();
                        int quantity = msg.readInt();
                        boolean express = msg.readBoolean();

                        System.out.println("\nReceived BytesMessage:");
                        System.out.println("  Order ID: " + orderId);
                        System.out.println("  Product: " + productName);
                        System.out.println("  Price: $" + price);
                        System.out.println("  Quantity: " + quantity);
                        System.out.println("  Express Shipping: " + express);
                        messageReceived.countDown();
                    } catch (javax.jms.JMSException e) {
                        System.err.println("Error reading BytesMessage: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("No message received. Ensure Kafka is running with share groups enabled.");
                }
            } catch (Exception e) {
                System.err.println("Receiver error: " + e.getMessage());
                e.printStackTrace();
            }
        });
        receiver.start();

        // Wait for receiver to subscribe
        receiverReady.await(10, TimeUnit.SECONDS);
        System.out.println("Receiver ready. Waiting 4s for share group join...\n");
        Thread.sleep(4000);

        // Send BytesMessage with structured order data
        try (JMSContext ctx = factory.createContext(JMSContext.AUTO_ACKNOWLEDGE)) {
            Queue queue = ctx.createQueue(queueName);
            BytesMessage msg = ctx.createBytesMessage();

            // Write order data as primitives
            int orderId = 12345;
            String productName = "Kafka Cluster License";
            double price = 999.99;
            int quantity = 2;
            boolean express = true;

            try {
                msg.writeInt(orderId);
                msg.writeUTF(productName);
                msg.writeDouble(price);
                msg.writeInt(quantity);
                msg.writeBoolean(express);

                ctx.createProducer().send(queue, msg);

                System.out.println("Sent BytesMessage:");
                System.out.println("  Order ID: " + orderId);
                System.out.println("  Product: " + productName);
                System.out.println("  Price: $" + price);
                System.out.println("  Quantity: " + quantity);
                System.out.println("  Express Shipping: " + express);
            } catch (javax.jms.JMSException e) {
                System.err.println("Error writing BytesMessage: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Wait for receiver to process message
        messageReceived.await(20, TimeUnit.SECONDS);
        receiver.join(25000);

        System.out.println("\nDone.");
    }
}
