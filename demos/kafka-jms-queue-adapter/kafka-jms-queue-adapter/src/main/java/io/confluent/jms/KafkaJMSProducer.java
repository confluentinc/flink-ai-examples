package io.confluent.jms;

import javax.jms.BytesMessage;
import javax.jms.CompletionListener;
import javax.jms.Destination;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.jms.JMSRuntimeException;

/**
 * JMS 2.0 JMSProducer backed by Kafka.
 */
class KafkaJMSProducer implements JMSProducer {

    private final KafkaQueueBackend backend;

    KafkaJMSProducer(KafkaQueueBackend backend) {
        this.backend = backend;
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        if (!(destination instanceof Queue q)) {
            throw new JMSRuntimeException("Only Queue destinations supported");
        }

        try {
            String queueName = q.getQueueName();
            byte[] body;
            String messageType;

            if (message instanceof TextMessage tm) {
                // TextMessage: convert text to UTF-8 bytes
                body = tm.getText() != null
                    ? tm.getText().getBytes(java.nio.charset.StandardCharsets.UTF_8)
                    : new byte[0];
                messageType = "TEXT";
            } else if (message instanceof BytesMessage bm) {
                // BytesMessage: extract raw bytes
                long bodyLength = bm.getBodyLength();
                body = new byte[(int) bodyLength];
                bm.reset();
                bm.readBytes(body);
                messageType = "BYTES";
            } else {
                throw new JMSRuntimeException(
                    "Only TextMessage and BytesMessage supported, got: " + message.getClass().getName());
            }

            backend.publish(queueName, null, body, messageType);
            return this;
        } catch (javax.jms.JMSException e) {
            // JMS-specific exception from getMessage methods or getQueueName()
            throw new JMSRuntimeException(e.getMessage(), e.getErrorCode(), e);
        } catch (RuntimeException e) {
            // Runtime exception from backend.publish() - preserve full context
            throw new JMSRuntimeException("Failed to send message: " + e.getMessage(), null, e);
        } catch (Exception e) {
            // Unexpected checked exception - wrap and preserve
            throw new JMSRuntimeException("Unexpected error sending message: " + e.getMessage(), null, e);
        }
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        return send(destination, new KafkaTextMessage(body, null, null));
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        try {
            BytesMessage bm = new KafkaBytesMessage(null, null, null);
            if (body != null && body.length > 0) {
                bm.writeBytes(body);
            }
            return send(destination, bm);
        } catch (javax.jms.JMSException e) {
            throw new JMSRuntimeException("Failed to create BytesMessage from byte array: " + e.getMessage(), e.getErrorCode(), e);
        }
    }

    @Override
    public JMSProducer send(Destination destination, java.io.Serializable body) {
        return send(destination, body != null ? body.toString() : "");
    }

    @Override
    public JMSProducer send(Destination destination, java.util.Map<String, Object> body) {
        throw new UnsupportedOperationException("MapMessage body not supported - use TextMessage");
    }

    @Override
    public JMSProducer setDisableMessageID(boolean value) {
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return false;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean value) {
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return false;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return javax.jms.DeliveryMode.PERSISTENT;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        return this;
    }

    @Override
    public int getPriority() {
        return 4;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        return this;
    }

    @Override
    public long getTimeToLive() {
        return 0;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return 0;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        throw new UnsupportedOperationException("Async send not supported");
    }

    @Override
    public CompletionListener getAsync() {
        return null;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        return this;
    }

    @Override
    public boolean propertyExists(String name) {
        return false;
    }

    @Override
    public boolean getBooleanProperty(String name) {
        return false;
    }

    @Override
    public byte getByteProperty(String name) {
        return 0;
    }

    @Override
    public short getShortProperty(String name) {
        return 0;
    }

    @Override
    public int getIntProperty(String name) {
        return 0;
    }

    @Override
    public long getLongProperty(String name) {
        return 0;
    }

    @Override
    public float getFloatProperty(String name) {
        return 0;
    }

    @Override
    public double getDoubleProperty(String name) {
        return 0;
    }

    @Override
    public String getStringProperty(String name) {
        return null;
    }

    @Override
    public Object getObjectProperty(String name) {
        return null;
    }

    @Override
    public java.util.Set<String> getPropertyNames() {
        return java.util.Set.of();
    }

    @Override
    public javax.jms.Destination getJMSReplyTo() {
        return null;
    }

    @Override
    public JMSProducer setJMSReplyTo(javax.jms.Destination destination) {
        return this;
    }

    @Override
    public String getJMSType() {
        return null;
    }

    @Override
    public JMSProducer setJMSType(String type) {
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return null;
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationID) {
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        return null;
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationID) {
        return this;
    }
}
