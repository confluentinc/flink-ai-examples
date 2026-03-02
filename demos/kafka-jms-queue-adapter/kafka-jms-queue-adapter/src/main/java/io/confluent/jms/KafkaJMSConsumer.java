package io.confluent.jms;

import javax.jms.JMSContext;
import javax.jms.JMSConsumer;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.jms.JMSRuntimeException;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JMS 2.0 JMSConsumer backed by Kafka (Queues for Kafka).
 */
class KafkaJMSConsumer implements JMSConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaJMSConsumer.class);

    private final KafkaQueueBackend backend;
    private final String queueName;
    private final int sessionMode;
    private final long listenerPollMs;
    private static final long DEFAULT_RECEIVE_TIMEOUT = 5000;

    private final AtomicReference<MessageListener> messageListener = new AtomicReference<>();
    private volatile Thread listenerThread;
    private volatile boolean closed;

    KafkaJMSConsumer(KafkaQueueBackend backend, String queueName, int sessionMode) {
        this.backend = backend;
        this.queueName = queueName;
        this.sessionMode = sessionMode;
        this.listenerPollMs = backend.getListenerPollIntervalMs();
    }

    @Override
    public String getMessageSelector() {
        return null;
    }

    @Override
    public MessageListener getMessageListener() {
        return messageListener.get();
    }

    @Override
    public void setMessageListener(MessageListener listener) {
        MessageListener prev = messageListener.getAndSet(listener);
        if (prev != null) {
            stopListenerThread();
        }
        if (listener != null && !closed) {
            startListenerThread(listener);
        }
    }

    @Override
    public Message receive() {
        return receive(DEFAULT_RECEIVE_TIMEOUT);
    }

    @Override
    public Message receive(long timeout) {
        if (messageListener.get() != null) {
            throw new JMSRuntimeException("Cannot call receive() when MessageListener is set - use one or the other");
        }
        return doReceive(timeout);
    }

    private Message doReceive(long timeout) {
        KafkaQueueBackend.ConsumedMessage cm = backend.consume(queueName, timeout);
        if (cm == null) {
            return null;
        }

        Message msg;
        if ("BYTES".equals(cm.messageType())) {
            // Create BytesMessage
            KafkaBytesMessage bytesMsg = new KafkaBytesMessage(
                cm.body(), queueName, cm.deliveryId(), cm.timestamp());
            if (sessionMode != JMSContext.AUTO_ACKNOWLEDGE) {
                bytesMsg.setConsumerForAck(this);
            }
            msg = bytesMsg;
        } else {
            // Create TextMessage (default for "TEXT" or absent header)
            String text = cm.bodyAsString();
            KafkaTextMessage textMsg = new KafkaTextMessage(
                text, queueName, cm.deliveryId(), cm.timestamp());
            if (sessionMode != JMSContext.AUTO_ACKNOWLEDGE) {
                textMsg.setConsumerForAck(this);
            }
            msg = textMsg;
        }

        if (sessionMode == JMSContext.AUTO_ACKNOWLEDGE) {
            backend.ack(queueName, cm.deliveryId());
        }

        return msg;
    }

    @Override
    public Message receiveNoWait() {
        return receive(0);
    }

    @Override
    public <T> T receiveBody(Class<T> c) {
        Message m = receive();
        if (m == null) return null;
        try {
            return m.getBody(c);
        } catch (javax.jms.JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> c, long timeout) {
        Message m = receive(timeout);
        if (m == null) return null;
        try {
            return m.getBody(c);
        } catch (javax.jms.JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> c) {
        Message m = receiveNoWait();
        if (m == null) return null;
        try {
            return m.getBody(c);
        } catch (javax.jms.JMSException e) {
            throw new JMSRuntimeException(e.getMessage(), null, e);
        }
    }

    @Override
    public void close() {
        closed = true;
        setMessageListener(null);
        // Backend is shared, no other cleanup needed
    }

    private void startListenerThread(MessageListener listener) {
        Thread t = new Thread(() -> {
            while (messageListener.get() == listener && !closed) {
                try {
                    Message msg = doReceive(listenerPollMs);
                    if (msg != null) {
                        listener.onMessage(msg);
                    }
                } catch (Exception e) {
                    if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    if (!closed && messageListener.get() == listener) {
                        log.warn("MessageListener threw exception for queue {}: {}", queueName, e.getMessage(), e);
                    }
                }
            }
        }, "jms-listener-" + queueName);
        t.setDaemon(true);
        t.start();
        listenerThread = t;
    }

    private void stopListenerThread() {
        Thread t = listenerThread;
        if (t != null && t.isAlive()) {
            // Interrupt BEFORE clearing reference to prevent race condition
            t.interrupt();
            listenerThread = null;
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            listenerThread = null;
        }
    }

    void acknowledge(KafkaTextMessage msg) {
        if (msg.getQueueName() != null && msg.getDeliveryId() != null) {
            backend.ack(msg.getQueueName(), msg.getDeliveryId());
        }
    }

    void acknowledge(KafkaBytesMessage msg) {
        if (msg.getQueueName() != null && msg.getDeliveryId() != null) {
            backend.ack(msg.getQueueName(), msg.getDeliveryId());
        }
    }
}
