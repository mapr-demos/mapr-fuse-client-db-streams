package com.mapr.fuse.service;

import com.mapr.fuse.client.ConfigReader;
import com.mapr.fuse.client.KafkaClient;
import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.entity.MessageRange;
import com.mapr.fuse.entity.TopicRange;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ReadDataService {

    private KafkaClient kafkaClient;
    private ConfigReader configReader;
    private TopicReader reader;
    private ConcurrentHashMap<TopicPartition, LinkedList<Integer>> topicSizeData;
    private MessageConfig messageConfig;

    public ReadDataService(final TopicReader reader, ConfigReader configReader) {
        this.reader = reader;
        this.configReader = configReader;
        messageConfig = new MessageConfig();
        kafkaClient = new KafkaClient();
        topicSizeData = new ConcurrentHashMap<>();
    }

    /**
     * Start tracking size of topic in the background thread.
     *
     * @param topicName topic to track
     * @return list of sizes of messages
     */
    public Integer requestTopicSizeData(final String topicName, final Integer partitionId) {
        TopicPartition partition = new TopicPartition(topicName, partitionId);
        updateMessageConfig();
        updatePartitionSize(partition);

        return topicSizeData.get(partition).stream()
                .mapToInt(Integer::intValue)
                .map(x -> x + getSeparatorsLength())
                .sum();
    }

    private void updatePartitionSize(final TopicPartition partition) {
        if (!topicSizeData.containsKey(partition)) {
            topicSizeData.put(partition, new LinkedList<>());
        }
        kafkaClient.readPartition(partition, topicSizeData.get(partition).size(), 200L)
                .forEach(record -> topicSizeData.get(partition)
                        .addLast(record.value().get().length + getSeparatorsLength()));
    }

    private void updateMessageConfig() {
        messageConfig = configReader.getLatestMessageConfig(1500L);
    }

    private Integer getSeparatorsLength() {
        return (messageConfig.getSeparator() + messageConfig.getStart() + messageConfig.getStop()).getBytes().length;
    }

    /**
     * The same as {@link TopicReader#readPartition(TopicPartition, long, long, long, MessageConfig)}
     *
     * @return byte array with records
     */
    public byte[] readRequiredBytesFromTopicPartition(final TopicPartition partition, final Long startOffset,
                                                      final Long numberOfBytes, final Long timeout) {
        TopicRange topicReadRange =
                calculateReadRange(partition, startOffset, numberOfBytes);

        Optional<byte[]> batchOfBytes = reader.readPartition(partition, topicReadRange.getStartOffset().getTopicOffset(),
                topicReadRange.getNumberOfMessages(), timeout, messageConfig);

        return batchOfBytes.map(bytes -> Arrays.copyOfRange(bytes, topicReadRange.getStartOffset().getOffsetFromStartMessage(),
                bytes.length - topicReadRange.getEndOffset().getMessageOffset())).orElseGet(() -> new byte[0]);
    }


    /**
     * Calculates read range based on offset and number of needed bytes
     */
    private TopicRange calculateReadRange(final TopicPartition partition, final Long startOffset, final Long endOffset) {
        List<Integer> messagesSizes = topicSizeData.get(partition);

        MessageRange startRange =
                calculateMessageRange(messagesSizes, startOffset);

        MessageRange endRange =
                calculateMessageRange(messagesSizes, endOffset);

        return new TopicRange(startRange, endRange);
    }

    private MessageRange calculateMessageRange(final List<Integer> messagesSizes, final Long endOffset) {
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
}
