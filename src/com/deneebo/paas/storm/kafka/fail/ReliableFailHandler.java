

package com.deneebo.paas.storm.kafka.fail;

import com.deneebo.paas.storm.kafka.util.KafkaMessageId;

import com.deneebo.paas.storm.kafka.fail.AbstractFailHandler;
import com.deneebo.paas.storm.kafka.fail.FailHandler;

/**
 * {@link FailHandler} implementation making tuple failure reliable: messages are always replayed, calls to
 * {@link #fail(com.deneebo.paas.storm.kafka.util.KafkaMessageId, byte[])} will cause an error.
 *
 * 
 */
public class ReliableFailHandler extends AbstractFailHandler {
    /**
     * Configuration identifier for the reliable failure policy ({@code "reliable"}).
     */
    public static final String IDENTIFIER = "reliable";

    @Override
    public boolean shouldReplay(final KafkaMessageId id) {
        // always replay the message, never call fail
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @throws IllegalStateException always; tuples should always replayed using reliable policy.
     */
    @Override
    public void fail(final KafkaMessageId id, final byte[] message) {
        throw new IllegalStateException("reliable failure policy unexpectedly made to deal with message failure");
    }

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }
}
