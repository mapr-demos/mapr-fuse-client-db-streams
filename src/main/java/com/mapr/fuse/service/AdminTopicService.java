package com.mapr.fuse.service;

import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
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
    public int countTopics(final String streamPath) {
        return admin.countTopics(streamPath);
    }

    @SneakyThrows
    public int getTopicPartitions(final String stream, final String topic) {
        return admin.getTopicDescriptor(stream, topic).getPartitions();
    }

    public void createStream(final String stream) throws IOException {
        admin.createStream(stream, Streams.newStreamDescriptor());
    }

    public void createTopic(final String stream, final String topic) throws IOException {
        admin.createTopic(stream, topic);
    }

    public void removeStream(final String stream) throws IOException {
        admin.deleteStream(stream);
    }

    public void removeTopic(final String stream, final String topic) throws IOException {
        admin.deleteTopic(stream, topic);
    }

    @SneakyThrows
    public StreamDescriptor getStreamDescriptor(String stream) {
        return admin.getStreamDescriptor(stream);
    }

}
