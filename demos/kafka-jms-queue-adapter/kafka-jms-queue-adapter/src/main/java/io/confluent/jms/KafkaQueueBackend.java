package io.confluent.jms;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kafka-backed queue service using Queues for Kafka (KIP-932) share groups.
 */
class KafkaQueueBackend {

    private final String bootstrapServers;
    private final String shareGroupPrefix;
    private final Properties baseProducerProps;
    private final Properties baseConsumerProps;

    private volatile KafkaProducer<String, byte[]> producer;
    private final Map<String, ConsumerPool> consumerPools = new ConcurrentHashMap<>();
    private final Map<String, InFlightDelivery> inFlightDeliveries = new ConcurrentHashMap<>();
    private final AtomicInteger consumerIdCounter = new AtomicInteger(0);

    KafkaQueueBackend(String bootstrapServers, String shareGroupPrefix) {
        this.bootstrapServers = bootstrapServers;
        this.shareGroupPrefix = shareGroupPrefix != null ? shareGroupPrefix : "jms-adapter-share-group";
        this.baseProducerProps = baseProducerProperties();
        this.baseConsumerProps = baseConsumerProperties();
    }

    private Properties baseProducerProperties() {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", bootstrapServers);
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "jms-adapter-producer");
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        addSaslIfConfigured(p);
        return p;
    }

    private Properties baseConsumerProperties() {
        Properties p = new Properties();
        p.setProperty("bootstrap.servers", bootstrapServers);
        p.setProperty("key.deserializer", StringDeserializer.class.getName());
        p.setProperty("value.deserializer", ByteArrayDeserializer.class.getName());
        p.setProperty("share.acknowledgement.mode", "explicit");
        p.setProperty("max.poll.records", "1");
        p.setProperty("max.poll.interval.ms", "300000");
        addSaslIfConfigured(p);
        return p;
    }

    private void addSaslIfConfigured(Properties p) {
        String apiKey = System.getenv("CONFLUENT_API_KEY");
        String apiSecret = System.getenv("CONFLUENT_API_SECRET");
        if (apiKey != null && apiSecret != null && !apiKey.isEmpty() && !apiSecret.isEmpty()) {
            p.setProperty("security.protocol", "SASL_SSL");
            p.setProperty("sasl.mechanism", "PLAIN");
            p.setProperty("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                apiKey, apiSecret));
        }
    }

    private synchronized KafkaProducer<String, byte[]> getProducer() {
        if (producer == null) {
            producer = new KafkaProducer<>(baseProducerProps);
        }
        return producer;
    }

    String publish(String queue, String key, byte[] body) {
        try {
            String k = key != null && !key.isEmpty() ? key : "msg-" + System.currentTimeMillis();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(queue, k, body);
            RecordMetadata meta = getProducer().send(record).get();
            return meta.partition() + ":" + meta.offset();
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish to queue " + queue, e);
        }
    }

    ConsumedMessage consume(String queue, long timeoutMs) {
        ConsumerPool pool = consumerPools.computeIfAbsent(queue, q -> new ConsumerPool(q, 10));
        PooledConsumer pc = pool.acquire();
        try {
            KafkaShareConsumer<String, byte[]> consumer = pc.consumer;
            // Retry polls: first poll often returns empty while consumer joins share group
            long remaining = timeoutMs;
            long pollMs = Math.min(2000, timeoutMs);
            while (remaining > 0) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(pollMs));
                if (!records.isEmpty()) {
                    ConsumerRecord<String, byte[]> record = records.iterator().next();
                    String deliveryId = record.partition() + ":" + record.offset();
                    InFlightDelivery ifd = new InFlightDelivery(queue, record, consumer, pc);
                    inFlightDeliveries.put(deliveryId, ifd);
                    return new ConsumedMessage(deliveryId, record.key(), record.value(), record.timestamp());
                }
                remaining -= pollMs;
            }
            return null;
        } catch (Exception e) {
            pool.release(pc);
            throw new RuntimeException("Failed to consume from queue " + queue, e);
        }
    }

    void ack(String queue, String deliveryId) {
        settle(queue, deliveryId, AcknowledgeType.ACCEPT);
    }

    void release(String queue, String deliveryId) {
        settle(queue, deliveryId, AcknowledgeType.RELEASE);
    }

    private void settle(String queue, String deliveryId, AcknowledgeType ackType) {
        InFlightDelivery ifd = inFlightDeliveries.remove(deliveryId);
        if (ifd == null) {
            throw new IllegalArgumentException("Unknown or already settled delivery: " + deliveryId);
        }
        if (!queue.equals(ifd.queue)) {
            inFlightDeliveries.put(deliveryId, ifd);
            throw new IllegalArgumentException("Delivery " + deliveryId + " belongs to queue " + ifd.queue);
        }
        try {
            ifd.consumer.acknowledge(ifd.record, ackType);
            ifd.consumer.commitSync();
        } finally {
            ifd.pooledConsumer.pool.release(ifd.pooledConsumer);
        }
    }

    void close() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
        for (ConsumerPool pool : consumerPools.values()) {
            pool.close();
        }
        consumerPools.clear();
        inFlightDeliveries.clear();
    }

    record ConsumedMessage(String deliveryId, String key, byte[] body, long timestamp) {
        String bodyAsString() {
            return body != null ? new String(body, StandardCharsets.UTF_8) : null;
        }
    }

    private static final class InFlightDelivery {
        final String queue;
        final ConsumerRecord<String, byte[]> record;
        final KafkaShareConsumer<String, byte[]> consumer;
        final PooledConsumer pooledConsumer;

        InFlightDelivery(String queue, ConsumerRecord<String, byte[]> record,
                         KafkaShareConsumer<String, byte[]> consumer,
                         PooledConsumer pooledConsumer) {
            this.queue = queue;
            this.record = record;
            this.consumer = consumer;
            this.pooledConsumer = pooledConsumer;
        }
    }

    private class ConsumerPool {
        final String queue;
        final int size;
        final java.util.Queue<PooledConsumer> available = new java.util.concurrent.ConcurrentLinkedQueue<>();
        final Map<PooledConsumer, Boolean> all = new ConcurrentHashMap<>();

        ConsumerPool(String queue, int size) {
            this.queue = queue;
            this.size = size;
        }

        PooledConsumer acquire() {
            PooledConsumer pc = available.poll();
            if (pc != null) return pc;
            if (all.size() < size) {
                pc = createConsumer();
                all.put(pc, Boolean.TRUE);
                return pc;
            }
            while (true) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted", e);
                }
                pc = available.poll();
                if (pc != null) return pc;
            }
        }

        void release(PooledConsumer pc) {
            available.offer(pc);
        }

        private PooledConsumer createConsumer() {
            Properties props = new Properties();
            props.putAll(baseConsumerProps);
            props.setProperty("group.id", shareGroupPrefix + "-" + queue);
            props.setProperty("client.id", "jms-adapter-" + queue + "-" + consumerIdCounter.incrementAndGet());
            KafkaShareConsumer<String, byte[]> consumer = new KafkaShareConsumer<>(props);
            consumer.subscribe(Collections.singletonList(queue));
            return new PooledConsumer(this, consumer);
        }

        void close() {
            for (PooledConsumer pc : all.keySet()) {
                pc.consumer.close();
            }
            all.clear();
            available.clear();
        }
    }

    private static final class PooledConsumer {
        final ConsumerPool pool;
        final KafkaShareConsumer<String, byte[]> consumer;

        PooledConsumer(ConsumerPool pool, KafkaShareConsumer<String, byte[]> consumer) {
            this.pool = pool;
            this.consumer = consumer;
        }
    }
}
