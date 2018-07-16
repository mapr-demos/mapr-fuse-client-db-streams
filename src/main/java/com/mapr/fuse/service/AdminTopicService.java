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
    public Set<String> getTopicNames(String streamPath) {
        return new HashSet<>(admin.listTopics(streamPath));
    }

    @SneakyThrows
    public int getTopicPartitions(String stream, String topic) {
        return admin.getTopicDescriptor(stream, topic).getPartitions();
    }

    @SneakyThrows
    public void createStream(String stream) {
        admin.createStream(stream, Streams.newStreamDescriptor());
    }

    @SneakyThrows
    public void createTopic(String stream, String topic) {
        admin.createTopic(stream, topic);
    }

    @SneakyThrows
    public void removeStream(String stream) {
        admin.deleteStream(stream);
    }

    @SneakyThrows
    public void removeTopic(String stream, String topic) {
        admin.deleteTopic(stream, topic);
    }

}
