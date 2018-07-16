package com.mapr.fuse.service;

import com.mapr.streams.Admin;
import com.mapr.streams.Streams;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class AdminTopicService {

    private final AdminClient adminClient;
    private final Admin admin;
    private final static String KAFKA_HOST = "kafkaHost";

    @SneakyThrows
    public AdminTopicService() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        Configuration conf = new Configuration();

        adminClient = AdminClient.create(consumerProps);
        admin = Streams.newAdmin(conf);
    }

    @SneakyThrows
    public Set<String> getTopicNames(String streamPath) {
        return adminClient.listTopics(streamPath).names().get();
    }

    @SneakyThrows
    public List<TopicPartitionInfo> getTopicPartitions(String topic) {
        return adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get().partitions();
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
