package com.mapr.fuse.client;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;

@Slf4j
public class KafkaClient {
    private final Map<UUID, MessageHandler> messageHandlers = new ConcurrentHashMap<>();
    private final Scheduler consumerScheduler = Schedulers.newSingle("KafkaConsumer-thread");
    private final LinkedBlockingDeque<Set<TopicPartition>> subscribeEvents = new LinkedBlockingDeque<>();
    private final KafkaConsumer<String, String> kafkaConsumer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Disposable consumerTask;
    //put kafka host here
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
        consumerTask = connect();
    }

    private Disposable connect() {
        return consumerScheduler.schedule(() -> {
            while (!closed.get()) {
                try {
                    List<Set<TopicPartition>> subEvents = new ArrayList<>();
                    if (subscribeEvents.drainTo(subEvents) > 0) {
                        Set<TopicPartition> topicPartitions = subEvents.stream()
                                .flatMap(Collection::stream)
                                .collect(toSet());
                        if (isNotEmpty(topicPartitions)) {
                            if (topicPartitions.size() > 10) {
                                log.debug("Subscribing to {} topicPartitions", topicPartitions.size());
                            } else {
                                log.debug("Subscribing to topicPartitions {}", topicPartitions);
                            }
                            kafkaConsumer.assign(topicPartitions);
                        }
                    }

                    if (kafkaConsumer.assignment().isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }

                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(500);
                    if (Objects.isNull(consumerRecords) || consumerRecords.isEmpty()) {
                        continue;
                    }
                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                        Set<TopicPartition> partitions = consumerRecords.partitions();
                        for (MessageHandler messageHandler : messageHandlers.values()) {
                            partitions.forEach(x -> messageHandler.consume(x, consumerRecord));
                        }
                    }
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
        kafkaConsumer.close();
    }

    public Flux<ConsumerRecord<String, String>> subscribe(Collection<TopicPartition> topicPartitions) {
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
        subscribeEvents.putLast(getCurrentlySubscribedTopicPartitions());
    }

    @SneakyThrows
    private void removeMessageHandler(UUID handlerId) {
        if (messageHandlers.remove(handlerId) != null) {
            subscribeEvents.putLast(getCurrentlySubscribedTopicPartitions());
        }
    }

    private Set<TopicPartition> getCurrentlySubscribedTopicPartitions() {
        return messageHandlers.values()
                .stream()
                .flatMap(MessageHandler::getTopicPartitions)
                .collect(toSet());
    }

    private interface MessageHandler {
        static MessageHandler of(Collection<TopicPartition> topicPartitions, Consumer<ConsumerRecord<String, String>> messageConsumer) {
            if (topicPartitions.size() != 1) {
                return new MultipleTopicsMessageHandler(newHashSet(topicPartitions), messageConsumer);
            }

            TopicPartition topicPartition = topicPartitions.iterator().next();
            return new SingleTopicMessageHandler(topicPartition, messageConsumer);
        }

        void consume(TopicPartition topicPartition, ConsumerRecord<String, String> message);

        Stream<TopicPartition> getTopicPartitions();
    }

    @RequiredArgsConstructor
    private static class SingleTopicMessageHandler implements MessageHandler {
        private final TopicPartition subscribedTopicPartition;
        private final Consumer<ConsumerRecord<String, String>> messageConsumer;

        @Override
        public void consume(TopicPartition topicPartition, ConsumerRecord<String, String> message) {
            if (subscribedTopicPartition.equals(topicPartition)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<TopicPartition> getTopicPartitions() {
            return Stream.of(subscribedTopicPartition);
        }
    }

    @RequiredArgsConstructor
    private static class MultipleTopicsMessageHandler implements MessageHandler {
        private final Set<TopicPartition> subscribedTopicPartitions;
        private final Consumer<ConsumerRecord<String, String>> messageConsumer;

        @Override
        public void consume(TopicPartition topicPartition, ConsumerRecord<String, String> message) {
            if (subscribedTopicPartitions.contains(topicPartition)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<TopicPartition> getTopicPartitions() {
            return subscribedTopicPartitions.stream();
        }
    }
}
