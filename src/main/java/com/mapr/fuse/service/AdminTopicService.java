package com.mapr.fuse.service;

import com.mapr.streams.Admin;
import com.mapr.streams.Streams;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Set;

@Slf4j
public class AdminTopicService {

    private final Admin admin;

    @SneakyThrows
    public AdminTopicService(Configuration configuration) {
        admin = Streams.newAdmin(configuration);
    }

    @SneakyThrows
    public boolean streamExists(final String stream) {
        return admin.streamExists(stream);
    }

    @SneakyThrows
    public Set<String> getTopicNames(final String streamPath) {
        return new HashSet<>(admin.listTopics(streamPath));
    }

    @SneakyThrows
    public int getTopicPartitions(final String stream, final String topic) {
        return admin.getTopicDescriptor(stream, topic).getPartitions();
    }

    @SneakyThrows
    public void createStream(final String stream) {
        admin.createStream(stream, Streams.newStreamDescriptor());
    }

    @SneakyThrows
    public void createTopic(final String stream, final String topic) {
        admin.createTopic(stream, topic);
    }

    @SneakyThrows
    public void removeStream(final String stream) {
        admin.deleteStream(stream);
    }

    @SneakyThrows
    public void removeTopic(final String stream, final String topic) {
        admin.deleteTopic(stream, topic);
    }

}
