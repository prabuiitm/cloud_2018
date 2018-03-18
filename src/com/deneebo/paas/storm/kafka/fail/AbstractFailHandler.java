
package com.deneebo.paas.storm.kafka.fail;

import java.util.Map;

import com.deneebo.paas.storm.kafka.util.KafkaMessageId;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.deneebo.paas.storm.kafka.fail.FailHandler;

/**
 * Abstract convenience implementation of the {@link FailHandler} interface.
 *
 * 
 */
public abstract class AbstractFailHandler implements FailHandler {
    @Override
    public abstract boolean shouldReplay(final KafkaMessageId id);

    @Override
    public void ack(final KafkaMessageId id) {
    }

    @Override
    public void fail(final KafkaMessageId id, final byte[] message) {
    }

    @Override
    public void open(final Map config, final TopologyContext topology, final SpoutOutputCollector collector) {
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void close() {
    }

    @Override
    public abstract String getIdentifier();
}
