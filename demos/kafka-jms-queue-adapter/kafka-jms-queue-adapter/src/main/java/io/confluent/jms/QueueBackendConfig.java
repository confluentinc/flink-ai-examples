package io.confluent.jms;

/**
 * Immutable configuration for KafkaQueueBackend.
 * Provides sensible defaults while allowing customization via builder pattern.
 */
public final class QueueBackendConfig {

    private final int consumerPoolSize;
    private final long consumerAcquireTimeoutMs;
    private final long inFlightDeliveryTimeoutMs;
    private final long producerSendTimeoutMs;
    private final long listenerPollIntervalMs;
    private final long cleanupIntervalMs;

    private QueueBackendConfig(Builder builder) {
        this.consumerPoolSize = builder.consumerPoolSize;
        this.consumerAcquireTimeoutMs = builder.consumerAcquireTimeoutMs;
        this.inFlightDeliveryTimeoutMs = builder.inFlightDeliveryTimeoutMs;
        this.producerSendTimeoutMs = builder.producerSendTimeoutMs;
        this.listenerPollIntervalMs = builder.listenerPollIntervalMs;
        this.cleanupIntervalMs = builder.cleanupIntervalMs;
    }

    /**
     * @return Maximum number of consumers per queue pool
     */
    public int getConsumerPoolSize() {
        return consumerPoolSize;
    }

    /**
     * @return Timeout in milliseconds when waiting for an available consumer from the pool
     */
    public long getConsumerAcquireTimeoutMs() {
        return consumerAcquireTimeoutMs;
    }

    /**
     * @return Timeout in milliseconds for in-flight deliveries to be auto-released if not acknowledged
     */
    public long getInFlightDeliveryTimeoutMs() {
        return inFlightDeliveryTimeoutMs;
    }

    /**
     * @return Timeout in milliseconds for producer send operations
     */
    public long getProducerSendTimeoutMs() {
        return producerSendTimeoutMs;
    }

    /**
     * @return Poll interval in milliseconds for asynchronous message listeners
     */
    public long getListenerPollIntervalMs() {
        return listenerPollIntervalMs;
    }

    /**
     * @return Interval in milliseconds for running the cleanup thread to check for expired deliveries
     */
    public long getCleanupIntervalMs() {
        return cleanupIntervalMs;
    }

    /**
     * Creates a new builder with default configuration values.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Returns the default configuration.
     */
    public static QueueBackendConfig defaults() {
        return builder().build();
    }

    public static final class Builder {
        private int consumerPoolSize = 10;
        private long consumerAcquireTimeoutMs = 30000; // 30 seconds
        private long inFlightDeliveryTimeoutMs = 300000; // 5 minutes
        private long producerSendTimeoutMs = 60000; // 60 seconds
        private long listenerPollIntervalMs = 1000; // 1 second
        private long cleanupIntervalMs = 30000; // 30 seconds

        private Builder() {
        }

        /**
         * Sets the maximum number of consumers per queue pool.
         * Default: 10
         */
        public Builder consumerPoolSize(int consumerPoolSize) {
            if (consumerPoolSize <= 0) {
                throw new IllegalArgumentException("consumerPoolSize must be positive");
            }
            this.consumerPoolSize = consumerPoolSize;
            return this;
        }

        /**
         * Sets the timeout when waiting for an available consumer from the pool.
         * Default: 30000 ms (30 seconds)
         */
        public Builder consumerAcquireTimeoutMs(long consumerAcquireTimeoutMs) {
            if (consumerAcquireTimeoutMs <= 0) {
                throw new IllegalArgumentException("consumerAcquireTimeoutMs must be positive");
            }
            this.consumerAcquireTimeoutMs = consumerAcquireTimeoutMs;
            return this;
        }

        /**
         * Sets the timeout for in-flight deliveries to be auto-released if not acknowledged.
         * Default: 300000 ms (5 minutes)
         */
        public Builder inFlightDeliveryTimeoutMs(long inFlightDeliveryTimeoutMs) {
            if (inFlightDeliveryTimeoutMs <= 0) {
                throw new IllegalArgumentException("inFlightDeliveryTimeoutMs must be positive");
            }
            this.inFlightDeliveryTimeoutMs = inFlightDeliveryTimeoutMs;
            return this;
        }

        /**
         * Sets the timeout for producer send operations.
         * Default: 60000 ms (60 seconds)
         */
        public Builder producerSendTimeoutMs(long producerSendTimeoutMs) {
            if (producerSendTimeoutMs <= 0) {
                throw new IllegalArgumentException("producerSendTimeoutMs must be positive");
            }
            this.producerSendTimeoutMs = producerSendTimeoutMs;
            return this;
        }

        /**
         * Sets the poll interval for asynchronous message listeners.
         * Default: 1000 ms (1 second)
         */
        public Builder listenerPollIntervalMs(long listenerPollIntervalMs) {
            if (listenerPollIntervalMs <= 0) {
                throw new IllegalArgumentException("listenerPollIntervalMs must be positive");
            }
            this.listenerPollIntervalMs = listenerPollIntervalMs;
            return this;
        }

        /**
         * Sets the interval for running the cleanup thread to check for expired deliveries.
         * Default: 30000 ms (30 seconds)
         */
        public Builder cleanupIntervalMs(long cleanupIntervalMs) {
            if (cleanupIntervalMs <= 0) {
                throw new IllegalArgumentException("cleanupIntervalMs must be positive");
            }
            this.cleanupIntervalMs = cleanupIntervalMs;
            return this;
        }

        /**
         * Builds an immutable QueueBackendConfig instance.
         */
        public QueueBackendConfig build() {
            return new QueueBackendConfig(this);
        }
    }
}
