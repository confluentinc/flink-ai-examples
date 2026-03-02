package io.confluent.jms;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * JMS BytesMessage backed by Kafka message bytes.
 * Supports reading and writing primitive types and byte arrays.
 */
class KafkaBytesMessage implements BytesMessage {

    // Write mode state
    private ByteArrayOutputStream writeBuffer;
    private DataOutputStream dataOut;

    // Read mode state
    private ByteArrayInputStream readBuffer;
    private DataInputStream dataIn;

    // Mode tracking
    private boolean readMode;
    private int bodyLength;

    // JMS metadata
    private final String queueName;
    private final String deliveryId;
    private final long timestamp;
    private KafkaJMSConsumer consumerForAck;

    /**
     * Constructor for creating new BytesMessage (write mode).
     */
    KafkaBytesMessage(byte[] body, String queueName, String deliveryId) {
        this(body, queueName, deliveryId, 0);
    }

    /**
     * Constructor for received messages or creating new messages.
     * If body is provided, starts in read mode. Otherwise, starts in write mode.
     */
    KafkaBytesMessage(byte[] body, String queueName, String deliveryId, long timestamp) {
        this.queueName = queueName;
        this.deliveryId = deliveryId;
        this.timestamp = timestamp;

        if (body != null && body.length > 0) {
            // Start in read mode for received messages
            this.readBuffer = new ByteArrayInputStream(body);
            this.dataIn = new DataInputStream(readBuffer);
            this.readMode = true;
            this.bodyLength = body.length;
        } else {
            // Start in write mode for new messages
            this.writeBuffer = new ByteArrayOutputStream();
            this.dataOut = new DataOutputStream(writeBuffer);
            this.readMode = false;
            this.bodyLength = 0;
        }
    }

    void setConsumerForAck(KafkaJMSConsumer consumer) {
        this.consumerForAck = consumer;
    }

    String getQueueName() {
        return queueName;
    }

    String getDeliveryId() {
        return deliveryId;
    }

    /**
     * Get message body as byte array for sending to Kafka.
     */
    byte[] getBodyAsBytes() {
        if (readMode && readBuffer != null) {
            // If in read mode, return the original bytes
            return readBuffer.readAllBytes();
        } else if (writeBuffer != null) {
            // If in write mode, return written bytes
            return writeBuffer.toByteArray();
        }
        return new byte[0];
    }

    /**
     * Switch from write mode to read mode.
     */
    private void switchToReadMode() {
        if (!readMode && writeBuffer != null) {
            byte[] data = writeBuffer.toByteArray();
            this.readBuffer = new ByteArrayInputStream(data);
            this.dataIn = new DataInputStream(readBuffer);
            this.bodyLength = data.length;
            this.readMode = true;
        }
    }

    @Override
    public long getBodyLength() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        return bodyLength;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readBoolean();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read boolean: " + e.getMessage());
        }
    }

    @Override
    public byte readByte() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readByte();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read byte: " + e.getMessage());
        }
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readUnsignedByte();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read unsigned byte: " + e.getMessage());
        }
    }

    @Override
    public short readShort() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readShort();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read short: " + e.getMessage());
        }
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readUnsignedShort();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read unsigned short: " + e.getMessage());
        }
    }

    @Override
    public char readChar() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readChar();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read char: " + e.getMessage());
        }
    }

    @Override
    public int readInt() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readInt();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read int: " + e.getMessage());
        }
    }

    @Override
    public long readLong() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readLong();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read long: " + e.getMessage());
        }
    }

    @Override
    public float readFloat() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readFloat();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read float: " + e.getMessage());
        }
    }

    @Override
    public double readDouble() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readDouble();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read double: " + e.getMessage());
        }
    }

    @Override
    public String readUTF() throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        try {
            return dataIn.readUTF();
        } catch (EOFException e) {
            throw new MessageEOFException("End of message stream");
        } catch (IOException e) {
            throw new JMSException("Failed to read UTF string: " + e.getMessage());
        }
    }

    @Override
    public int readBytes(byte[] value) throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        if (value == null) {
            throw new NullPointerException("byte array cannot be null");
        }
        try {
            int bytesRead = dataIn.read(value);
            return bytesRead == -1 ? -1 : bytesRead;
        } catch (IOException e) {
            throw new JMSException("Failed to read bytes: " + e.getMessage());
        }
    }

    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        if (!readMode) {
            throw new MessageNotReadableException("Message is in write mode");
        }
        if (value == null) {
            throw new NullPointerException("byte array cannot be null");
        }
        if (length < 0 || length > value.length) {
            throw new IndexOutOfBoundsException("Invalid length: " + length);
        }
        try {
            int bytesRead = dataIn.read(value, 0, length);
            return bytesRead == -1 ? -1 : bytesRead;
        } catch (IOException e) {
            throw new JMSException("Failed to read bytes: " + e.getMessage());
        }
    }

    @Override
    public void writeBoolean(boolean value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeBoolean(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write boolean: " + e.getMessage());
        }
    }

    @Override
    public void writeByte(byte value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeByte(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write byte: " + e.getMessage());
        }
    }

    @Override
    public void writeShort(short value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeShort(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write short: " + e.getMessage());
        }
    }

    @Override
    public void writeChar(char value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeChar(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write char: " + e.getMessage());
        }
    }

    @Override
    public void writeInt(int value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeInt(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write int: " + e.getMessage());
        }
    }

    @Override
    public void writeLong(long value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeLong(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write long: " + e.getMessage());
        }
    }

    @Override
    public void writeFloat(float value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeFloat(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write float: " + e.getMessage());
        }
    }

    @Override
    public void writeDouble(double value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeDouble(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write double: " + e.getMessage());
        }
    }

    @Override
    public void writeUTF(String value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        try {
            dataOut.writeUTF(value != null ? value : "");
        } catch (IOException e) {
            throw new JMSException("Failed to write UTF string: " + e.getMessage());
        }
    }

    @Override
    public void writeBytes(byte[] value) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        if (value == null) {
            throw new NullPointerException("byte array cannot be null");
        }
        try {
            dataOut.write(value);
        } catch (IOException e) {
            throw new JMSException("Failed to write bytes: " + e.getMessage());
        }
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        if (readMode) {
            throw new MessageNotWriteableException("Message is in read mode");
        }
        if (value == null) {
            throw new NullPointerException("byte array cannot be null");
        }
        try {
            dataOut.write(value, offset, length);
        } catch (IOException e) {
            throw new JMSException("Failed to write bytes: " + e.getMessage());
        }
    }

    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException("value cannot be null");
        }
        if (value instanceof Boolean) {
            writeBoolean((Boolean) value);
        } else if (value instanceof Byte) {
            writeByte((Byte) value);
        } else if (value instanceof Short) {
            writeShort((Short) value);
        } else if (value instanceof Character) {
            writeChar((Character) value);
        } else if (value instanceof Integer) {
            writeInt((Integer) value);
        } else if (value instanceof Long) {
            writeLong((Long) value);
        } else if (value instanceof Float) {
            writeFloat((Float) value);
        } else if (value instanceof Double) {
            writeDouble((Double) value);
        } else if (value instanceof String) {
            writeUTF((String) value);
        } else if (value instanceof byte[]) {
            writeBytes((byte[]) value);
        } else {
            throw new MessageFormatException("Cannot write object of type: " + value.getClass().getName());
        }
    }

    @Override
    public void reset() throws JMSException {
        if (!readMode) {
            switchToReadMode();
        } else if (readBuffer != null) {
            // Reset read position to beginning
            try {
                readBuffer.reset();
                this.dataIn = new DataInputStream(readBuffer);
            } catch (Exception e) {
                // If reset fails, recreate from beginning
                byte[] data = readBuffer.readAllBytes();
                this.readBuffer = new ByteArrayInputStream(data);
                this.dataIn = new DataInputStream(readBuffer);
            }
        }
    }

    @Override
    public void acknowledge() throws JMSException {
        if (consumerForAck != null && queueName != null && deliveryId != null) {
            consumerForAck.acknowledge(this);
        }
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return c != null && c == byte[].class;
    }

    @Override
    public void clearBody() throws JMSException {
        this.writeBuffer = new ByteArrayOutputStream();
        this.dataOut = new DataOutputStream(writeBuffer);
        this.readBuffer = null;
        this.dataIn = null;
        this.bodyLength = 0;
        this.readMode = false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getBody(Class<T> c) throws JMSException {
        if (c == byte[].class) {
            return (T) getBodyAsBytes();
        }
        throw new MessageFormatException("BytesMessage body is not assignable to " + c.getName());
    }

    // Property methods (no-op, same as TextMessage)
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

    // JMS header methods (same pattern as TextMessage)
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
        return false;
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
}
