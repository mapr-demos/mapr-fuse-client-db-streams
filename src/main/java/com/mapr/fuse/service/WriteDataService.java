package com.mapr.fuse.service;

import com.mapr.fuse.client.TopicWriter;

public class WriteDataService {

    private final TopicWriter writer;

    public WriteDataService(final TopicWriter writer) {
        this.writer = writer;
    }

    /**
     * The same as {@link TopicWriter#writeToTopic(String, byte[], Long)}
     */
    public void writeToPartition(String streamTopicPath, final byte[] message, Long timeout) {
        writer.writeToTopic(streamTopicPath, message, timeout);
    }
}
