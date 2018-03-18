

package com.deneebo.paas.storm.kafka.fail;

import java.io.Serializable;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import com.deneebo.paas.storm.kafka.fail.FailHandler;
import com.deneebo.paas.storm.kafka.util.KafkaMessageId;

/**
 * Handler interface to implement a user-defined failure policy. Used by the
 * {@link com.deneebo.paas.storm.kafka.KafkaSpout} to determine whether failed messages should be replayed. Messages not
 * to be replayed are provided to the handler to deal with, at which point the
 * {@link com.deneebo.paas.storm.kafka.KafkaSpout} will continue as if the message was processed correctly. This makes it
 * the handler's responsibility to implement the failure policy (e.g. log failure, add to error topic on kafka, ...).
 *
 *
 */
public interface FailHandler extends Serializable {
    /**
     * Queries the handler whether the message emitted as {@code id} should be replayed.
     * NB: messages that should not be replayed are provided to the handler through {@link #fail}, but are considered
     * processed from the kafka point of view and offsets will be committed as such. The {@link FailHandler} is
     * responsible for dealing with the message when the {@link com.deneebo.paas.storm.kafka.KafkaSpout} should not emit
     * it again.
     *
     * @param id The failed id.
     * @return Whether the kafka message emitted as {@code id} should be replayed.
     */
    boolean shouldReplay(KafkaMessageId id);

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when a tuple is acknowledged by the topology.
     *
     * @param id The message that was acknowledged by the topology.
     */
    void ack(KafkaMessageId id);

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when a tuple is failed by the topology and
     * {@link #shouldReplay(com.deneebo.paas.storm.kafka.util.KafkaMessageId)} indicates it should *not* be replayed.
     *
     * @param id      The failed id.
     * @param message The failed message.
     */
    void fail(KafkaMessageId id, byte[] message);

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when
     * {@link com.deneebo.paas.storm.kafka.KafkaSpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)}
     * is called on it to allow the {@link FailHandler} to update its state.
     *
     * @param config    The configuration as passed to
     *                  {@link com.deneebo.paas.storm.kafka.KafkaSpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)}.
     * @param topology  The {@link TopologyContext} as passed to
     *                  {@link com.deneebo.paas.storm.kafka.KafkaSpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)}.
     * @param collector The {@link SpoutOutputCollector} as passed to
     *                  {@link com.deneebo.paas.storm.kafka.KafkaSpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)}.
     */
    void open(Map config, TopologyContext topology, SpoutOutputCollector collector);

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when
     * {@link com.deneebo.paas.storm.kafka.KafkaSpout#activate()} is called on it.
     */
    void activate();

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when
     * {@link com.deneebo.paas.storm.kafka.KafkaSpout#deactivate()} is called on it.
     */
    void deactivate();

    /**
     * Called by the {@link com.deneebo.paas.storm.kafka.KafkaSpout} when
     * {@link com.deneebo.paas.storm.kafka.KafkaSpout#close()} is called on it.
     */
    void close();

    /**
     * Called by {@link com.deneebo.paas.storm.kafka.KafkaSpout} on opening the spout to log the failure policy used.
     */
    String getIdentifier();
}
