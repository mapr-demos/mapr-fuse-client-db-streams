package com.mapr.fuse.client;

import com.google.common.base.Stopwatch;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.utils.Bytes;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

@Slf4j
public class KafkaClient {
    private final KafkaConsumer<Bytes, Bytes> kafkaConsumer;
    private final Map<UUID, MessageHandler> messageHandlers = new ConcurrentHashMap<>();
    private final Scheduler consumerScheduler = Schedulers.newSingle("KafkaConsumer-thread");
    private final LinkedBlockingDeque<Set<TopicPartition>> subscribeEvents = new LinkedBlockingDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Disposable consumerTask;

    public KafkaClient() {
        Map<String, Object> consumerProps = new HashMap<>();
//        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_HOST);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "fuse-client");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
        consumerTask = connect();
    }

    private Disposable connect() {
        return consumerScheduler.schedule(() -> {
            while (!closed.get()) {
                try {
                    List<Set<TopicPartition>> subEvents = new ArrayList<>();
                    if (subscribeEvents.drainTo(subEvents) > 0) {
                        Set<TopicPartition> partitions = subEvents.stream()
                                .flatMap(Collection::stream)
                                .collect(toSet());
                        if (isNotEmpty(partitions)) {
                            if (partitions.size() > 10) {
                                log.debug("Subscribing to {} partitions", partitions.size());
                            } else {
                                log.debug("Subscribing to partitions {}", partitions);
                            }
                            kafkaConsumer.assign(partitions);
                        }
                    }

                    if (kafkaConsumer.assignment().isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }

                    ConsumerRecords<Bytes, Bytes> consumerRecords = kafkaConsumer.poll(500);
                    if (Objects.isNull(consumerRecords) || consumerRecords.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<Bytes, Bytes> consumerRecord : consumerRecords) {
                        String topicName = consumerRecord.topic();
                        for (MessageHandler messageHandler : messageHandlers.values()) {
                            messageHandler.consume(topicName, consumerRecord);
                        }
                    }
                    kafkaConsumer.commitSync();
                } catch (Exception e) {
                    log.error("Unexpected error: {}", e.getMessage(), e);
                }
            }
        });
    }

    @SneakyThrows
    public void close() {
        closed.set(true);
        while (!consumerTask.isDisposed()) {
            Thread.sleep(100);
        }
        kafkaConsumer.commitSync();
        kafkaConsumer.close();
    }

    private Flux<ConsumerRecord<Bytes, Bytes>> subscribe(Collection<TopicPartition> topicPartitions) {
        return Flux.create(sink -> {
            UUID handlerId = UUID.randomUUID();
            MessageHandler messageHandler = MessageHandler.of(topicPartitions, sink::next);
            putMessageHandler(handlerId, messageHandler);
            sink.onDispose(() -> removeMessageHandler(handlerId));
        });
    }

    @SneakyThrows
    private void putMessageHandler(UUID handlerId, MessageHandler messageHandler) {
        messageHandlers.put(handlerId, messageHandler);
        subscribeEvents.putLast(getCurrentlySubscribedTopics());
    }

    @SneakyThrows
    private void removeMessageHandler(UUID handlerId) {
        if (messageHandlers.remove(handlerId) != null) {
            subscribeEvents.putLast(getCurrentlySubscribedTopics());
        }
    }

    private Set<TopicPartition> getCurrentlySubscribedTopics() {
        return messageHandlers.values()
                .stream()
                .flatMap(MessageHandler::getTopicPartitions)
                .collect(toSet());
    }

    private interface MessageHandler {
        static MessageHandler of(Collection<TopicPartition> topics, Consumer<ConsumerRecord<Bytes, Bytes>> messageConsumer) {
            if (topics.size() != 1) {
                return new MultipleTopicsMessageHandler(newHashSet(topics), messageConsumer);
            }

            TopicPartition topic = topics.iterator().next();
            return new SingleTopicMessageHandler(topic, messageConsumer);
        }

        void consume(String topic, ConsumerRecord<Bytes, Bytes> message);

        Stream<TopicPartition> getTopicPartitions();
    }

    @RequiredArgsConstructor
    private static class SingleTopicMessageHandler implements MessageHandler {
        private final TopicPartition subscribedTopic;
        private final Consumer<ConsumerRecord<Bytes, Bytes>> messageConsumer;

        @Override
        public void consume(String topic, ConsumerRecord<Bytes, Bytes> message) {
            if (subscribedTopic.equals(topic)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<TopicPartition> getTopicPartitions() {
            return Stream.of(subscribedTopic);
        }
    }

    @RequiredArgsConstructor
    private static class MultipleTopicsMessageHandler implements MessageHandler {
        private final Set<TopicPartition> subscribedTopics;
        private final Consumer<ConsumerRecord<Bytes, Bytes>> messageConsumer;

        @Override
        public void consume(String topic, ConsumerRecord<Bytes, Bytes> message) {
            if (subscribedTopics.contains(topic)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<TopicPartition> getTopicPartitions() {
            return subscribedTopics.stream();
        }
    }

    public List<ConsumerRecord<Bytes, Bytes>> readPartition(final TopicPartition partition, final long offset,
            final long timeout) {
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
