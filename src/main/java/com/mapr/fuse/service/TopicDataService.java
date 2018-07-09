package com.mapr.fuse.service;

import com.mapr.fuse.client.KafkaClient;
import com.mapr.fuse.entity.MessageRange;
import com.mapr.fuse.entity.TopicRange;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Schedulers;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.singletonList;

@Slf4j
public class TopicDataService {

    private KafkaClient kafkaClient;
    private ConcurrentHashMap<String, LinkedList<Integer>> topicSizeData;

    public TopicDataService() {
        kafkaClient = new KafkaClient();
        topicSizeData = new ConcurrentHashMap<>();
    }

    public List<Integer> requestTopicSizeData(String topicName) {
        if (!topicSizeData.containsKey(topicName)) {
            startReadingTopic(topicName);
        }
        return topicSizeData.get(topicName);
    }

    public TopicRange numberOfMessagesToRead(String topic, Long startOffset, Long endOffset) {
        List<Integer> messagesSizes = topicSizeData.get(topic);

        MessageRange startRange =
                getElemRange(messagesSizes, startOffset);

        MessageRange endRange =
                getElemRange(messagesSizes, endOffset);

        return new TopicRange(startRange, endRange);
    }

    private MessageRange getElemRange(List<Integer> messagesSizes, Long endOffset) {
        int sum = 0;
        int messOffset = 0;
        int i = 0;
        for (; i < messagesSizes.size(); i++) {
            sum += messagesSizes.get(i);
            if (sum >= endOffset) {
                break;
            }
        }
        if (sum > endOffset && endOffset != 0) {
            messOffset = (int) (sum - endOffset);
        }
        return new MessageRange(i, messOffset, messagesSizes.get(i));
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
