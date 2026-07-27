package io.adzubla.blocks.idempotency.messaging.jms;

import jakarta.jms.BytesMessage;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import org.springframework.jms.core.JmsTemplate;

import java.util.Enumeration;

/**
 * Publishes a delivery to its dead-letter destination (source destination
 * name + a configured suffix, {@code idempotency.jms.dead-letter-suffix},
 * default {@code ".DLQ"}) instead of invoking the listener - the terminal
 * cases from the PRD §5 action-mapping table (collision, missing/invalid key)
 * where no consumer-side retry can resolve the delivery.
 *
 * <p>Unlike RabbitMQ's broker-native reject-without-requeue, the JMS API has
 * no primitive that skips a delivery straight to a dead-letter destination
 * without going through the broker's own redelivery-count-then-DLQ policy -
 * so, like the Kafka module's {@code KafkaDeadLetterPublisher}, this
 * explicitly republishes the message onto a separate destination via a {@link
 * JmsTemplate} and the original delivery is then acked (not redelivered), the
 * same "app-managed republish" mechanism the PRD's "or {@code
 * JmsListenerErrorHandler} routing to one" wording anticipates.
 */
public class JmsDeadLetterPublisher {

    private final JmsTemplate jmsTemplate;
    private final String deadLetterSuffix;

    public JmsDeadLetterPublisher(JmsTemplate jmsTemplate, String deadLetterSuffix) {
        this.jmsTemplate = jmsTemplate;
        this.deadLetterSuffix = deadLetterSuffix;
    }

    /** Republishes {@code message}'s body/properties, unchanged, to {@code destinationName}'s dead-letter destination. */
    public void publish(Message message, String destinationName) {
        if (jmsTemplate == null) {
            throw new IllegalStateException("Idempotency dead-letter routing required for destination=" + destinationName
                    + " but no JmsTemplate bean is configured to publish to " + deadLetterDestination(destinationName));
        }
        jmsTemplate.send(deadLetterDestination(destinationName), session -> copy(message, session));
    }

    private String deadLetterDestination(String destinationName) {
        return destinationName + deadLetterSuffix;
    }

    private static Message copy(Message original, Session session) throws JMSException {
        Message copy = copyBody(original, session);
        Enumeration<?> propertyNames = original.getPropertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = (String) propertyNames.nextElement();
            copy.setObjectProperty(name, original.getObjectProperty(name));
        }
        return copy;
    }

    private static Message copyBody(Message original, Session session) throws JMSException {
        if (original instanceof TextMessage text) {
            return session.createTextMessage(text.getText());
        }
        if (original instanceof BytesMessage bytes) {
            // reset() rewinds the read cursor so the full body is available to read here,
            // regardless of whether fingerprinting already read (and reset) it earlier.
            bytes.reset();
            byte[] buffer = new byte[(int) bytes.getBodyLength()];
            bytes.readBytes(buffer);
            bytes.reset();
            BytesMessage copyMessage = session.createBytesMessage();
            copyMessage.writeBytes(buffer);
            return copyMessage;
        }
        return session.createMessage();
    }
}
