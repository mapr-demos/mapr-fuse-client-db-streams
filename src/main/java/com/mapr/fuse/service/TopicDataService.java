package com.mapr.fuse.service;

import com.mapr.fuse.client.KafkaClient;
import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.entity.MessageRange;
import com.mapr.fuse.entity.TopicRange;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonList;

@Slf4j
public class TopicDataService {

    private KafkaClient kafkaClient;
    private TopicReader reader;
    private ConcurrentHashMap<String, LinkedList<Integer>> topicSizeData;

    public TopicDataService(final TopicReader reader) {
        this.reader = reader;
        kafkaClient = new KafkaClient();
        topicSizeData = new ConcurrentHashMap<>();
    }

    /**
     * Start tracking size of topic in the background thread.
     *
     * @param topicName topic to track
     * @return list of sizes of messages
     */
    public List<Integer> requestTopicSizeData(String topicName) {
        if (!topicSizeData.containsKey(topicName)) {
            startReadingTopic(topicName);
        }
        return topicSizeData.get(topicName);
    }

    /**
     * The same as {@link TopicReader#read(String, long, long, long)}
     *
     * @return byte array with records
     */
    public byte[] readRequiredBytesFromTopic(String topic, Long startOffset, Long numberOfBytes, Long timeout) {
        TopicRange topicReadRange =
                calculateReadRange(topic, startOffset, numberOfBytes);

        Optional<byte[]> batchOfBytes = reader.read(topic, topicReadRange.getStartOffset().getTopicOffset(),
                topicReadRange.getNumberOfMessages(), timeout);

        return batchOfBytes.map(bytes -> Arrays.copyOfRange(bytes, topicReadRange.getStartOffset().getOffsetFromStartMessage(),
                bytes.length - topicReadRange.getEndOffset().getMessageOffset())).orElseGet(() -> new byte[0]);
    }


    /**
     * Calculates read range based on offset and number of needed bytes
     */
    private TopicRange calculateReadRange(String topic, Long startOffset, Long endOffset) {
        List<Integer> messagesSizes = topicSizeData.get(topic);

        MessageRange startRange =
                calculateMessageRange(messagesSizes, startOffset);

        MessageRange endRange =
                calculateMessageRange(messagesSizes, endOffset);

        return new TopicRange(startRange, endRange);
    }

    private MessageRange calculateMessageRange(List<Integer> messagesSizes, Long endOffset) {
        int sum = 0;
        int messOffset = 0;
        int i = 0;
        for (; i < messagesSizes.size(); i++) {
            sum += messagesSizes.get(i);
            if (sum >= endOffset) {
                break;
            }
        }

        int amountMessages = i;
        if (i > 0) {
            i--;
        }

        if (sum > endOffset && endOffset != 0) {
            messOffset = (int) (sum - endOffset);
        }
        return new MessageRange(amountMessages, messOffset, messagesSizes.get(i));
    }

    private void startReadingTopic(String topicName) {
        topicSizeData.put(topicName, new LinkedList<>());
        kafkaClient.subscribe(singletonList(topicName))
                .doOnNext(record -> topicSizeData.get(topicName).addLast(record.value().getBytes().length))
                .doOnError(error -> {
                    log.error("Error while reading topic {}", topicName);
                    topicSizeData.remove(topicName);
                })
                .subscribeOn(Schedulers.elastic())
                .subscribe();
    }
}
