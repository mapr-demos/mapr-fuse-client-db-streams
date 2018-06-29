package com.mapr.fuse.client;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class TopicReader {

    private final Scheduler consumerScheduler = Schedulers.newElastic("KafkaReader-thread");
    private final LinkedBlockingDeque<Set<String>> subscribeEvents = new LinkedBlockingDeque<>();
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final static String KAFKA_HOST = "kafkaHost";


    public TopicReader() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client-reader");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
    }

    private List<ConsumerRecord<String, String>> read(String topic, long offset, long amount, long timeout) {
        final AtomicBoolean closed = new AtomicBoolean(false);
        List<ConsumerRecord<String, String>> records = new LinkedList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        TopicPartition partition = new TopicPartition(topic, 0);
        kafkaConsumer.seek(partition, offset);
        consumerScheduler.schedule(() -> {
            while (!closed.get()) {
                try {
                    if (kafkaConsumer.subscription().isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }

                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(500);
                    if (Objects.isNull(consumerRecords) || consumerRecords.isEmpty()) {
                        continue;
                    }
                    records.addAll(consumerRecords.records(partition));
                    kafkaConsumer.seek(partition, offset + records.size());
                    if (records.size() >= amount || stopwatch.elapsed(TimeUnit.MILLISECONDS) >= timeout) {
                        closed.set(true);
                    }
                } catch (Exception e) {
                    log.error("Unexpected error: {}", e.getMessage(), e);
                }
            }
        });
        return records.stream()
                .limit(amount)
                .collect(Collectors.toList());
    }
}
