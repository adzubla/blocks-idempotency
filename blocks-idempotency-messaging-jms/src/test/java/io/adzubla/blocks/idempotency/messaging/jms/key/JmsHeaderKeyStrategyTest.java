package io.adzubla.blocks.idempotency.messaging.jms.key;

import jakarta.jms.JMSException;
import jakarta.jms.JMSRuntimeException;
import jakarta.jms.Message;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JmsHeaderKeyStrategyTest {

    private static final String PROPERTY = "IdempotencyKey";

    @Test
    void resolvesTheRawKeyFromTheStringProperty() throws JMSException {
        Message message = mock(Message.class);
        when(message.getStringProperty(PROPERTY)).thenReturn("key-1");

        assertThat(JmsHeaderKeyStrategy.resolve(message, PROPERTY)).contains("key-1");
    }

    @Test
    void absentWhenThePropertyIsMissing() throws JMSException {
        Message message = mock(Message.class);
        when(message.getStringProperty(PROPERTY)).thenReturn(null);

        assertThat(JmsHeaderKeyStrategy.resolve(message, PROPERTY)).isEmpty();
    }

    @Test
    void absentWhenThePropertyIsBlank() throws JMSException {
        Message message = mock(Message.class);
        when(message.getStringProperty(PROPERTY)).thenReturn("   ");

        assertThat(JmsHeaderKeyStrategy.resolve(message, PROPERTY)).isEqualTo(Optional.empty());
    }

    @Test
    void wrapsAJmsExceptionAsUnchecked() throws JMSException {
        Message message = mock(Message.class);
        when(message.getStringProperty(PROPERTY)).thenThrow(new JMSException("broker gone"));

        assertThatThrownBy(() -> JmsHeaderKeyStrategy.resolve(message, PROPERTY))
                .isInstanceOf(JMSRuntimeException.class)
                .hasMessageContaining(PROPERTY);
    }
}
