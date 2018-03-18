

package com.deneebo.paas.storm.kafka.fail;

import com.deneebo.paas.storm.kafka.util.KafkaMessageId;

import com.deneebo.paas.storm.kafka.fail.AbstractFailHandler;
import com.deneebo.paas.storm.kafka.fail.FailHandler;

/**
 * {@link FailHandler} implementation making tuple failure unreliable: messages are never replayed and calls to
 * {@link #fail(com.deneebo.paas.storm.kafka.util.KafkaMessageId, byte[])} are ignored.
 *
 */
public class UnreliableFailHandler extends AbstractFailHandler {
    /**
     * Configuration identifier for the unreliable failure policy ({@code "unreliable"}).
     */
    public static final String IDENTIFIER = "unreliable";

    @Override
    public boolean shouldReplay(final KafkaMessageId id) {
        // never replay a message, ignore calls to fail; lose the message in time and space
        return false;
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }
}
