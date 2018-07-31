package com.mapr.fuse.client;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicWriter {

    private final KafkaProducer<byte[], byte[]> kafkaProducer;


    public TopicWriter() {
        kafkaProducer = new KafkaProducer<>(getSetupProperties());
    }

    private Map<String, Object> getSetupProperties() {
        Map<String, Object> producerConfig = new HashMap<>();
//        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return producerConfig;
    }

    /**
     * Used to write messages to a topic. If write operation is slow, It will throw timeout exception.
     *
     * @param streamTopicName stream and topic name in mapr streams format 'streamName':'topicName'
     * @param message         message to send
     * @param timeout         timeout for operation
     */
    @SneakyThrows
    public void writeToTopic(final String streamTopicName, final byte[] message, final Long timeout) {
        ProducerRecord<byte[], byte[]> rec = new ProducerRecord<>(streamTopicName, message);
        kafkaProducer.send(rec).get(timeout, TimeUnit.MILLISECONDS);
    }
}
