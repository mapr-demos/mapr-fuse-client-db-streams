package com.mapr.fuse.client;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mapr.fuse.service.TopicDataService.MESSAGE_PATTERN;

@Slf4j
public class TopicReader {

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

    /**
     * Used to read needed amount of messages from topic. If it is not possible to read (less messages available) timeout
     * will brake loop
     *
     * @param topic       topic to read from
     * @param partitionId partition of topic to read from
     * @param offset      initial offset
     * @param amount      amount of messages to read
     * @param timeout     timeout to brake the loop if it is not possible te read needed amount of messages
     * @return List of records
     */
    public Optional<byte[]> readPartition(String topic, int partitionId, long offset, long amount, long timeout) {
        final AtomicBoolean closed = new AtomicBoolean(false);
        long currentPosition = offset;
        List<ConsumerRecord<String, String>> records = new LinkedList<>();
        Stopwatch stopwatch = Stopwatch.createStarted();
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        TopicPartition partition = new TopicPartition(topic, partitionId);
        while (!closed.get()) {
            try {
                if (kafkaConsumer.subscription().isEmpty()) {
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

                if (records.size() >= amount || stopwatch.elapsed(TimeUnit.MILLISECONDS) >= timeout) {
                    closed.set(true);
                }
            } catch (UnknownTopicOrPartitionException ignored) {
            } catch (Exception e) {
                log.error("Unexpected error: {}", e.getMessage(), e);
            }
        }
        kafkaConsumer.unsubscribe();
        return records.stream()
                .limit(amount)
                .map(record -> String.format(MESSAGE_PATTERN, record.value().getBytes().length, record.value()).getBytes())
                .reduce(ArrayUtils::addAll);
    }
}
