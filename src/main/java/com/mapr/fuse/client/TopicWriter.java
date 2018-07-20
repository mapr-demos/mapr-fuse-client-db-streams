package com.mapr.fuse.client;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TopicWriter {

    private final KafkaProducer<byte[], byte[]> kafkaProducer;
    private final static String KAFKA_HOST = "kafkaHost";


    public TopicWriter() {
        kafkaProducer = new KafkaProducer<>(getSetupProperties());
    }

    private Map<String, Object> getSetupProperties() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client-reader");
        consumerProps.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        consumerProps.put("key.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerProps;
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
