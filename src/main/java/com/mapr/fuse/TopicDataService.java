package com.mapr.fuse;

import com.mapr.fuse.client.KafkaClient;
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

    public List<Integer> reqestTopicSizeData(String topicName) {
        if (!topicSizeData.containsKey(topicName)) {
            startReadingTopic(topicName);
        }
        return topicSizeData.get(topicName);
    }

    public Integer numberOfMessagesToRead(String topic, Long startOffset, Long endOffset) {
        List<Integer> messagesSizes = topicSizeData.get(topic);
        long sum = 0;
        int startElement = 0;
        for (int i = 0; i < messagesSizes.size(); i++) {
            sum += messagesSizes.get(i);
            if (sum >= startOffset) {
                startElement = i - 1;
                break;
            }
        }
        int endElement = startElement;
        for (int i = startElement; i < messagesSizes.size(); i++) {
            sum += messagesSizes.get(i);
            endElement = i;
            if (sum >= endOffset) {
                endElement = i - 1;
                break;
            }
        }
        if (startElement == endElement) {
            return endElement;
        }
        return endElement - startElement;
    }

    private void startReadingTopic(String topicName) {
        topicSizeData.put(topicName, new LinkedList<>());
        kafkaClient.subscribe(singletonList(topicName))
                .doOnNext(record -> topicSizeData.get(topicName).addLast(record.value().getBytes().length))
                .subscribeOn(Schedulers.elastic())
                .doOnError(error -> {
                    log.error("Error while reading topic {}", topicName);
                    topicSizeData.remove(topicName);
                })
                .subscribe();
    }
}
