package io.confluent.jms;

import javax.jms.Queue;

/**
 * JMS Queue backed by a Kafka topic (queue name = topic name).
 */
public class KafkaQueue implements Queue {

    private final String queueName;

    public KafkaQueue(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String getQueueName() throws javax.jms.JMSException {
        return queueName;
    }
}
