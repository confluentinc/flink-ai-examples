package io.confluent.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

/**
 * JMS ConnectionFactory for Kafka (Queues for Kafka).
 * Create via KafkaConnectionFactory.create(bootstrapServers).
 */
public class KafkaConnectionFactory implements ConnectionFactory {

    private final String bootstrapServers;
    private final String shareGroupPrefix;

    private KafkaConnectionFactory(String bootstrapServers, String shareGroupPrefix) {
        this.bootstrapServers = bootstrapServers;
        this.shareGroupPrefix = shareGroupPrefix;
    }

    public static KafkaConnectionFactory create(String bootstrapServers) {
        return create(bootstrapServers, null);
    }

    public static KafkaConnectionFactory create(String bootstrapServers, String shareGroupPrefix) {
        return new KafkaConnectionFactory(bootstrapServers, shareGroupPrefix);
    }

    @Override
    public JMSContext createContext() {
        return createContext(JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        return createContext(null, null, sessionMode);
    }

    @Override
    public JMSContext createContext(String userName, String password) {
        return createContext(userName, password, JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Override
    public JMSContext createContext(String userName, String password, int sessionMode) {
        KafkaQueueBackend backend = new KafkaQueueBackend(bootstrapServers, shareGroupPrefix);
        return new KafkaJMSContext(backend, sessionMode);
    }

    @Override
    public Connection createConnection() throws JMSException {
        throw new UnsupportedOperationException("Use createContext() for JMS 2.0");
    }

    @Override
    public Connection createConnection(String userName, String password) throws JMSException {
        throw new UnsupportedOperationException("Use createContext() for JMS 2.0");
    }
}
