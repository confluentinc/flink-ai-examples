package io.confluent.jms;

import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.JMSConsumer;
import javax.jms.Queue;
import javax.jms.JMSRuntimeException;

/**
 * JMS 2.0 JMSContext backed by Kafka (Queues for Kafka).
 */
class KafkaJMSContext implements JMSContext {

    private final KafkaQueueBackend backend;
    private final int sessionMode;
    private boolean closed;

    KafkaJMSContext(KafkaQueueBackend backend, int sessionMode) {
        this.backend = backend;
        this.sessionMode = sessionMode;
    }

    @Override
    public JMSContext createContext(int sessionMode) throws JMSRuntimeException {
        throw new UnsupportedOperationException("Nested context not supported");
    }

    @Override
    public JMSProducer createProducer() {
        checkClosed();
        return new KafkaJMSProducer(backend);
    }

    @Override
    public JMSConsumer createConsumer(javax.jms.Destination destination) {
        return createConsumer(destination, null);
    }

    @Override
    public JMSConsumer createConsumer(javax.jms.Destination destination, String messageSelector) {
        if (messageSelector != null && !messageSelector.isEmpty()) {
            throw new UnsupportedOperationException("Message selectors not supported");
        }
        if (!(destination instanceof Queue q)) {
            throw new JMSRuntimeException("Only Queue destinations supported");
        }
        checkClosed();
        try {
            return new KafkaJMSConsumer(backend, q.getQueueName(), sessionMode);
        } catch (Exception e) {
            throw new JMSRuntimeException(e.getMessage() != null ? e.getMessage() : e.getClass().getName(), null, e);
        }
    }

    @Override
    public Queue createQueue(String queueName) {
        checkClosed();
        return new KafkaQueue(queueName);
    }

    @Override
    public javax.jms.TextMessage createTextMessage() {
        checkClosed();
        return new KafkaTextMessage(null, null, null);
    }

    @Override
    public javax.jms.TextMessage createTextMessage(String text) {
        checkClosed();
        return new KafkaTextMessage(text, null, null);
    }

    @Override
    public javax.jms.BytesMessage createBytesMessage() {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.MapMessage createMapMessage() {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.Message createMessage() {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.ObjectMessage createObjectMessage() {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.ObjectMessage createObjectMessage(java.io.Serializable object) {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.StreamMessage createStreamMessage() {
        throw new UnsupportedOperationException("Only TextMessage supported - use createTextMessage()");
    }

    @Override
    public javax.jms.Topic createTopic(String topicName) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createConsumer(javax.jms.Destination destination, String messageSelector, boolean noLocal) {
        return createConsumer(destination, messageSelector);
    }

    @Override
    public JMSConsumer createDurableConsumer(javax.jms.Topic topic, String name) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createDurableConsumer(javax.jms.Topic topic, String name, String messageSelector, boolean noLocal) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createSharedConsumer(javax.jms.Topic topic, String sharedSubscriptionName) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createSharedConsumer(javax.jms.Topic topic, String sharedSubscriptionName, String messageSelector) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(javax.jms.Topic topic, String name) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(javax.jms.Topic topic, String name, String messageSelector) {
        throw new UnsupportedOperationException("Topics not supported - queues only");
    }

    @Override
    public javax.jms.QueueBrowser createBrowser(Queue queue) {
        throw new UnsupportedOperationException("Queue browsing not supported");
    }

    @Override
    public javax.jms.QueueBrowser createBrowser(Queue queue, String messageSelector) {
        throw new UnsupportedOperationException("Queue browsing not supported");
    }

    @Override
    public javax.jms.TemporaryQueue createTemporaryQueue() {
        throw new UnsupportedOperationException("Temporary queues not supported");
    }

    @Override
    public javax.jms.TemporaryTopic createTemporaryTopic() {
        throw new UnsupportedOperationException("Temporary topics not supported");
    }

    @Override
    public void unsubscribe(String name) {
        throw new UnsupportedOperationException("Durable subscriptions not supported");
    }

    @Override
    public void acknowledge() {
        // No-op for AUTO_ACKNOWLEDGE; CLIENT_ACKNOWLEDGE uses message.acknowledge()
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            backend.close();
        }
    }

    @Override
    public void start() {
        // No-op - we're always "started"
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("Stop not supported");
    }

    @Override
    public void setClientID(String clientID) {
        // No-op
    }

    @Override
    public String getClientID() {
        return null;
    }

    @Override
    public void setExceptionListener(javax.jms.ExceptionListener listener) {
        // No-op for narrow scope
    }

    @Override
    public javax.jms.ExceptionListener getExceptionListener() {
        return null;
    }

    @Override
    public javax.jms.ConnectionMetaData getMetaData() {
        throw new UnsupportedOperationException("Metadata not supported");
    }

    @Override
    public boolean getAutoStart() {
        return true;
    }

    @Override
    public void setAutoStart(boolean autoStart) {
        // No-op
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public boolean getTransacted() {
        return false;
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("Transactions not supported");
    }

    @Override
    public void rollback() {
        throw new UnsupportedOperationException("Transactions not supported");
    }

    @Override
    public void recover() {
        throw new UnsupportedOperationException("Recover not supported");
    }

    private void checkClosed() {
        if (closed) {
            throw new JMSRuntimeException("Context is closed");
        }
    }
}
