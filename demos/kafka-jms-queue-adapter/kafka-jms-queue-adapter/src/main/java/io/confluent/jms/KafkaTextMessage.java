package io.confluent.jms;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import java.nio.charset.StandardCharsets;

/**
 * JMS TextMessage backed by Kafka message bytes.
 */
class KafkaTextMessage implements TextMessage {

    private String text;
    private final String queueName;
    private final String deliveryId;
    private final long timestamp;
    private KafkaJMSConsumer consumerForAck;

    KafkaTextMessage(String text, String queueName, String deliveryId) {
        this(text, queueName, deliveryId, 0);
    }

    KafkaTextMessage(String text, String queueName, String deliveryId, long timestamp) {
        this.text = text;
        this.queueName = queueName;
        this.deliveryId = deliveryId;
        this.timestamp = timestamp;
    }

    void setConsumerForAck(KafkaJMSConsumer consumer) {
        this.consumerForAck = consumer;
    }

    @Override
    public String getText() throws JMSException {
        return text;
    }

    @Override
    public void setText(String string) throws JMSException {
        this.text = string;
    }

    @Override
    public void acknowledge() throws JMSException {
        if (consumerForAck != null && queueName != null && deliveryId != null) {
            consumerForAck.acknowledge(this);
        }
    }

    String getQueueName() {
        return queueName;
    }

    String getDeliveryId() {
        return deliveryId;
    }

    byte[] getBodyAsBytes() {
        return text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException { /* no-op */ }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException { /* no-op */ }

    @Override
    public void setShortProperty(String name, short value) throws JMSException { /* no-op */ }

    @Override
    public void setIntProperty(String name, int value) throws JMSException { /* no-op */ }

    @Override
    public void setLongProperty(String name, long value) throws JMSException { /* no-op */ }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException { /* no-op */ }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException { /* no-op */ }

    @Override
    public void setStringProperty(String name, String value) throws JMSException { /* no-op */ }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException { /* no-op */ }

    @Override
    public java.util.Enumeration<String> getPropertyNames() throws JMSException {
        return java.util.Collections.emptyEnumeration();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return false;
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return false;
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return 0;
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return null;
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return null;
    }

    @Override
    public void clearProperties() throws JMSException {
        /* no-op */
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return deliveryId;
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        /* no-op - provider-assigned on receive */
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        /* no-op - provider-assigned on receive */
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return null;
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
        /* no-op */
    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        /* no-op */
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return null;
    }

    @Override
    public javax.jms.Destination getJMSReplyTo() throws JMSException {
        return null;
    }

    @Override
    public void setJMSReplyTo(javax.jms.Destination replyTo) throws JMSException {
        /* no-op */
    }

    @Override
    public javax.jms.Destination getJMSDestination() throws JMSException {
        return queueName != null ? new KafkaQueue(queueName) : null;
    }

    @Override
    public void setJMSDestination(javax.jms.Destination destination) throws JMSException {
        /* no-op - provider-assigned on receive */
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return javax.jms.DeliveryMode.PERSISTENT;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        /* no-op */
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return false;  /* Kafka share groups do not expose redelivery count */
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        /* no-op - provider-assigned on receive */
    }

    @Override
    public String getJMSType() throws JMSException {
        return null;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        /* no-op */
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        /* no-op */
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        /* no-op */
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return 4;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        /* no-op */
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return c != null && (c == String.class || c == byte[].class);
    }

    @Override
    public void clearBody() throws JMSException {
        text = null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (c == String.class) {
            return (T) (text != null ? text : "");
        }
        if (c == byte[].class) {
            return (T) getBodyAsBytes();
        }
        throw new JMSException("TextMessage body is not assignable to " + c.getName());
    }
}
