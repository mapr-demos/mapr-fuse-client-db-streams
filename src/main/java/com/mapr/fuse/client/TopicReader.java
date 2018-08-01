package com.mapr.fuse.client;

import com.google.common.base.Stopwatch;
import com.mapr.fuse.dto.MessageConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TopicReader {

    private final KafkaConsumer<Bytes, Bytes> kafkaConsumer;
    private final static String KAFKA_HOST = "kafkaHost";

    public TopicReader() {
        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client-reader");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
    }

    /**
     * Used to read needed amount of messages from topic. If it is not possible to read (less messages available) timeout
     * will brake loop
     *
     * @param partition partition of topic to read from
     * @param offset    initial offset
     * @param amount    amount of messages to read
     * @param timeout   timeout to brake the loop if it is not possible te read needed amount of messages
     * @return List of records
     */
    public List<ConsumerRecord<Bytes, Bytes>> readPartition(final TopicPartition partition, final long offset,
            final long amount, final long timeout) {
        final AtomicBoolean closed = new AtomicBoolean(false);
        long currentPosition = offset;
        List<ConsumerRecord<Bytes, Bytes>> records = new LinkedList<>();
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

                ConsumerRecords<Bytes, Bytes> consumerRecords = kafkaConsumer.poll(500);
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
        return records;
    }

    public List<ConsumerRecord<Bytes, Bytes>> readPartition(final TopicPartition partition, final long offset,
            final long timeout) {
        return readPartition(partition, offset, 1000L, timeout);
    }

    public Optional<byte[]> readAndFormat(final TopicPartition partition, final long offset,
            final long amount, final long timeout, MessageConfig config) {
        return readPartition(partition, offset, amount, timeout).stream()
                .limit(amount)
                .map(record -> formatMessage(config, new String(record.value().get())).getBytes())
                .reduce(ArrayUtils::addAll);
    }

    private String formatMessage(MessageConfig messageConfig, String message) {
        return String.format("%s%s%s%s",
                messageConfig.getStart(), message, messageConfig.getStop(), messageConfig.getSeparator());
    }
}
