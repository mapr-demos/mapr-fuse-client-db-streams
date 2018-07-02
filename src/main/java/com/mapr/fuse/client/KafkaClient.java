package com.mapr.fuse.client;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.*;
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
    private final LinkedBlockingDeque<Set<String>> subscribeEvents = new LinkedBlockingDeque<>();
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
                    List<Set<String>> subEvents = new ArrayList<>();
                    if (subscribeEvents.drainTo(subEvents) > 0) {
                        Set<String> topics = subEvents.stream()
                                .flatMap(Collection::stream)
                                .collect(toSet());
                        if (isNotEmpty(topics)) {
                            if (topics.size() > 10) {
                                log.debug("Subscribing to {} topics", topics.size());
                            } else {
                                log.debug("Subscribing to topics {}", topics);
                            }
                            kafkaConsumer.subscribe(topics);
                        }
                    }

                    if (kafkaConsumer.subscription().isEmpty()) {
                        Thread.sleep(100);
                        continue;
                    }

                    ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(500);
                    if (Objects.isNull(consumerRecords) || consumerRecords.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
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

    public Flux<ConsumerRecord<String, String>> subscribe(Collection<String> topicNames) {
        return Flux.create(sink -> {
            UUID handlerId = UUID.randomUUID();
            MessageHandler messageHandler = MessageHandler.of(topicNames, sink::next);
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

    private Set<String> getCurrentlySubscribedTopics() {
        return messageHandlers.values()
                .stream()
                .flatMap(MessageHandler::getTopics)
                .collect(toSet());
    }

    private interface MessageHandler {
        static MessageHandler of(Collection<String> topics, Consumer<ConsumerRecord<String, String>> messageConsumer) {
            if (topics.size() != 1) {
                return new MultipleTopicsMessageHandler(newHashSet(topics), messageConsumer);
            }

            String topic = topics.iterator().next();
            return new SingleTopicMessageHandler(topic, messageConsumer);
        }

        void consume(String topic, ConsumerRecord<String, String> message);

        Stream<String> getTopics();
    }

    @RequiredArgsConstructor
    private static class SingleTopicMessageHandler implements MessageHandler {
        private final String subscribedTopic;
        private final Consumer<ConsumerRecord<String, String>> messageConsumer;

        @Override
        public void consume(String topic, ConsumerRecord<String, String> message) {
            if (subscribedTopic.equals(topic)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<String> getTopics() {
            return Stream.of(subscribedTopic);
        }
    }

    @RequiredArgsConstructor
    private static class MultipleTopicsMessageHandler implements MessageHandler {
        private final Set<String> subscribedTopics;
        private final Consumer<ConsumerRecord<String, String>> messageConsumer;

        @Override
        public void consume(String topic, ConsumerRecord<String, String> message) {
            if (subscribedTopics.contains(topic)) {
                messageConsumer.accept(message);
            }
        }

        @Override
        public Stream<String> getTopics() {
            return subscribedTopics.stream();
        }
    }
}
