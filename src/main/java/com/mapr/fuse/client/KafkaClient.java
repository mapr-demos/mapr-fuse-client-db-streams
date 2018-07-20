package com.mapr.fuse.client;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class KafkaClient {
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final static String KAFKA_HOST = "kafkaHost";

    public KafkaClient() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
    }

    public List<ConsumerRecord<String, String>> readPartition(final TopicPartition partition, final long offset,
                                                              final long timeout) {
        final AtomicBoolean closed = new AtomicBoolean(false);
        long currentPosition = offset;
        List<ConsumerRecord<String, String>> records = new LinkedList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        kafkaConsumer.assign(Collections.singletonList(partition));
        while (!closed.get()) {
            try {
                if (kafkaConsumer.assignment().isEmpty()) {
                    Thread.sleep(100);
                    if (stopwatch.elapsed(TimeUnit.MILLISECONDS) >= timeout) {
                        closed.set(true);
                    } else continue;
                }

                kafkaConsumer.seek(partition, currentPosition);

                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(500);
                if (Objects.isNull(consumerRecords) || consumerRecords.isEmpty()) {
                    if (stopwatch.elapsed(TimeUnit.MILLISECONDS) >= timeout) {
                        closed.set(true);
                    } else continue;
                }
                records.addAll(consumerRecords.records(partition));
                currentPosition += consumerRecords.count();

                if (stopwatch.elapsed(TimeUnit.MILLISECONDS) >= timeout) {
                    closed.set(true);
                }
            } catch (UnknownTopicOrPartitionException ignored) {
            } catch (Exception e) {
                log.error("Unexpected error: {}", e.getMessage(), e);
            }
        }
        kafkaConsumer.unsubscribe();
        return records;
    }
}
