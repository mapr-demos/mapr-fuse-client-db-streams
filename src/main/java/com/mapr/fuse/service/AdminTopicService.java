package com.mapr.fuse.service;

import com.mapr.streams.Admin;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class AdminTopicService {

    private final Admin admin;

    public AdminTopicService(Configuration configuration) throws IOException {
        admin = Streams.newAdmin(configuration);
    }

    public boolean streamExists(final Path stream) throws IOException {
        return admin.streamExists(stream.toString());
    }

    public Set<String> getTopicNames(final Path streamPath) throws IOException {
        return new HashSet<>(admin.listTopics(streamPath.toString()));
    }

    public int countTopics(final Path streamPath) throws IOException {
        return admin.countTopics(streamPath.toString());
    }

    public int getTopicPartitions(final Path stream, final String topic) throws IOException {
        return admin.getTopicDescriptor(stream.toString(), topic).getPartitions();
    }

    public void createStream(final Path stream) throws IOException {
        admin.createStream(stream.toString(), Streams.newStreamDescriptor());
    }

    public void createTopic(final Path stream, final String topic) throws IOException {
        admin.createTopic(stream.toString(), topic);
    }

    public void removeStream(final Path stream) throws IOException {
        admin.deleteStream(stream.toString());
    }

    public void removeTopic(final Path stream, final String topic) throws IOException {
        admin.deleteTopic(stream.toString(), topic);
    }

    public StreamDescriptor getStreamDescriptor(Path stream) throws IOException {
        return admin.getStreamDescriptor(stream.toString());
    }

}
