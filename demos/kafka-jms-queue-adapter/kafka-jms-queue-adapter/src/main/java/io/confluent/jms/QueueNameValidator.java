package io.confluent.jms;

import java.util.regex.Pattern;

/**
 * Validates queue names according to Kafka topic naming rules.
 * Queue names map to Kafka topic names and must follow Kafka topic naming conventions.
 */
final class QueueNameValidator {

    // Kafka topic naming rules: max 249 characters, legal characters are a-z, A-Z, 0-9, . _ -
    private static final int MAX_NAME_LENGTH = 249;
    private static final Pattern VALID_CHARS = Pattern.compile("^[a-zA-Z0-9._-]+$");

    private QueueNameValidator() {
        // Utility class - prevent instantiation
    }

    /**
     * Validates a queue name according to Kafka topic naming rules.
     *
     * @param queueName the queue name to validate
     * @throws IllegalArgumentException if the queue name is invalid
     */
    static void validate(String queueName) {
        if (queueName == null) {
            throw new IllegalArgumentException("Queue name cannot be null");
        }

        if (queueName.isEmpty()) {
            throw new IllegalArgumentException("Queue name cannot be empty");
        }

        if (queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("Queue name cannot be whitespace only");
        }

        if (queueName.length() > MAX_NAME_LENGTH) {
            throw new IllegalArgumentException(
                String.format("Queue name exceeds maximum length of %d characters: '%s'",
                    MAX_NAME_LENGTH, queueName));
        }

        if (!VALID_CHARS.matcher(queueName).matches()) {
            throw new IllegalArgumentException(
                String.format("Queue name contains invalid characters. " +
                    "Only alphanumeric characters, dots (.), underscores (_), and hyphens (-) are allowed: '%s'",
                    queueName));
        }

        // Kafka reserved names
        if (".".equals(queueName) || "..".equals(queueName)) {
            throw new IllegalArgumentException("Queue name cannot be '.' or '..'");
        }
    }
}
