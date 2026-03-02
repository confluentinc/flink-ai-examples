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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka-backed queue service using Queues for Kafka (KIP-932) share groups.
 */
class KafkaQueueBackend {

    private static final Logger log = LoggerFactory.getLogger(KafkaQueueBackend.class);

    private final String bootstrapServers;
    private final String shareGroupPrefix;
    private final Properties baseProducerProps;
    private final Properties baseConsumerProps;
    private final QueueBackendConfig config;

    private volatile KafkaProducer<String, byte[]> producer;
    private final Map<String, ConsumerPool> consumerPools = new ConcurrentHashMap<>();
    private final Map<String, InFlightDelivery> inFlightDeliveries = new ConcurrentHashMap<>();
    private final AtomicInteger consumerIdCounter = new AtomicInteger(0);
    private final ScheduledExecutorService cleanupExecutor;
    private volatile boolean closed = false;

    KafkaQueueBackend(String bootstrapServers, String shareGroupPrefix) {
        this(bootstrapServers, shareGroupPrefix, QueueBackendConfig.defaults());
    }

    KafkaQueueBackend(String bootstrapServers, String shareGroupPrefix, QueueBackendConfig config) {
        this.bootstrapServers = bootstrapServers;
        this.shareGroupPrefix = shareGroupPrefix != null ? shareGroupPrefix : "jms-adapter-share-group";
        this.config = config != null ? config : QueueBackendConfig.defaults();
        this.baseProducerProps = baseProducerProperties();
        this.baseConsumerProps = baseConsumerProperties();

        // Start cleanup thread for expired in-flight deliveries
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "jms-adapter-cleanup");
            t.setDaemon(true);
            return t;
        });
        this.cleanupExecutor.scheduleAtFixedRate(
            this::cleanupExpiredDeliveries,
            config.getCleanupIntervalMs(),
            config.getCleanupIntervalMs(),
            TimeUnit.MILLISECONDS
        );
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
        QueueNameValidator.validate(queue);

        try {
            String k = key != null && !key.isEmpty() ? key : "msg-" + System.currentTimeMillis();
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(queue, k, body);
            RecordMetadata meta = getProducer().send(record).get(config.getProducerSendTimeoutMs(), TimeUnit.MILLISECONDS);
            return meta.partition() + ":" + meta.offset();
        } catch (TimeoutException e) {
            throw new RuntimeException(String.format(
                "Timeout publishing to queue '%s' after %d ms", queue, config.getProducerSendTimeoutMs()), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing to queue " + queue, e);
        } catch (Exception e) {
            throw new RuntimeException("Failed to publish to queue " + queue, e);
        }
    }

    ConsumedMessage consume(String queue, long timeoutMs) {
        QueueNameValidator.validate(queue);

        ConsumerPool pool = consumerPools.computeIfAbsent(queue, q -> new ConsumerPool(q, config.getConsumerPoolSize()));
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
                    InFlightDelivery ifd = new InFlightDelivery(queue, record, consumer, pc, System.currentTimeMillis());
                    inFlightDeliveries.put(deliveryId, ifd);
                    log.debug("Consumer acquired for queue '{}', delivery ID: {}", queue, deliveryId);
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
            log.debug("Settled delivery {} on queue '{}' with {}", deliveryId, queue, ackType);
        } finally {
            ifd.pooledConsumer.pool.release(ifd.pooledConsumer);
        }
    }

    /**
     * Cleanup thread method that releases expired in-flight deliveries.
     * This prevents consumer pool exhaustion from messages that are never acknowledged.
     */
    private void cleanupExpiredDeliveries() {
        if (closed) {
            return;
        }

        long now = System.currentTimeMillis();
        long expirationThreshold = now - config.getInFlightDeliveryTimeoutMs();

        inFlightDeliveries.entrySet().removeIf(entry -> {
            String deliveryId = entry.getKey();
            InFlightDelivery ifd = entry.getValue();

            if (ifd.createdAtMs < expirationThreshold) {
                try {
                    log.warn("Auto-releasing expired in-flight delivery {} on queue '{}' (age: {} ms)",
                        deliveryId, ifd.queue, now - ifd.createdAtMs);
                    ifd.consumer.acknowledge(ifd.record, AcknowledgeType.RELEASE);
                    ifd.consumer.commitSync();
                } catch (Exception e) {
                    log.error("Failed to auto-release expired delivery {} on queue '{}'",
                        deliveryId, ifd.queue, e);
                } finally {
                    ifd.pooledConsumer.pool.release(ifd.pooledConsumer);
                }
                return true; // Remove from map
            }
            return false; // Keep in map
        });
    }

    long getListenerPollIntervalMs() {
        return config.getListenerPollIntervalMs();
    }

    void close() {
        closed = true;

        // Shutdown cleanup executor
        if (cleanupExecutor != null && !cleanupExecutor.isShutdown()) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

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
        final long createdAtMs;

        InFlightDelivery(String queue, ConsumerRecord<String, byte[]> record,
                         KafkaShareConsumer<String, byte[]> consumer,
                         PooledConsumer pooledConsumer,
                         long createdAtMs) {
            this.queue = queue;
            this.record = record;
            this.consumer = consumer;
            this.pooledConsumer = pooledConsumer;
            this.createdAtMs = createdAtMs;
        }
    }

    private class ConsumerPool {
        final String queue;
        final int size;
        final LinkedBlockingQueue<PooledConsumer> available = new LinkedBlockingQueue<>();
        final Map<PooledConsumer, Boolean> all = new ConcurrentHashMap<>();

        ConsumerPool(String queue, int size) {
            this.queue = queue;
            this.size = size;
        }

        PooledConsumer acquire() {
            PooledConsumer pc = available.poll();
            if (pc != null) {
                log.debug("Reusing pooled consumer for queue '{}'", queue);
                return pc;
            }

            // Try to create a new consumer if pool not at capacity
            if (all.size() < size) {
                pc = createConsumer();
                all.put(pc, Boolean.TRUE);
                log.debug("Created new consumer for queue '{}' (pool size: {}/{})", queue, all.size(), size);
                return pc;
            }

            // Pool is exhausted, wait for a consumer to become available
            try {
                log.debug("Consumer pool exhausted for queue '{}', waiting up to {} ms", queue, config.getConsumerAcquireTimeoutMs());
                pc = available.poll(config.getConsumerAcquireTimeoutMs(), TimeUnit.MILLISECONDS);
                if (pc == null) {
                    throw new RuntimeException(String.format(
                        "Consumer pool exhausted for queue '%s': all %d consumers in use and none became available within %d ms",
                        queue, size, config.getConsumerAcquireTimeoutMs()));
                }
                return pc;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for available consumer for queue " + queue, e);
            }
        }

        void release(PooledConsumer pc) {
            available.offer(pc);
            log.debug("Released consumer back to pool for queue '{}'", queue);
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
