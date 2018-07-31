package com.mapr.fuse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.entity.MessageRange;
import com.mapr.fuse.entity.TopicRange;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ReadDataService {

    private TopicReader topicReader;
    private ConcurrentHashMap<TopicPartition, LinkedList<Integer>> topicSizeData;
    private MessageConfig messageConfig;

    public ReadDataService() {
        messageConfig = new MessageConfig();
        topicReader = new TopicReader();
        topicSizeData = new ConcurrentHashMap<>();
    }

    public Integer requestTopicSizeData(final String topicName, final Integer partitionId) {
        TopicPartition partition = new TopicPartition(topicName, partitionId);
        updateMessageConfig();
        updatePartitionSize(partition);

        return topicSizeData.get(partition).stream()
                .mapToInt(Integer::intValue)
                .map(x -> x + getSeparatorsLength())
                .sum();
    }

    public byte[] readRequiredBytesFromTopicPartition(final TopicPartition partition, final Long startOffset,
            final Long numberOfBytes, final Long timeout) {
        TopicRange topicReadRange =
                calculateReadRange(partition, startOffset, numberOfBytes);

        Optional<byte[]> batchOfBytes = topicReader.readAndFormat(partition, topicReadRange.getStartOffset().getTopicOffset(),
                topicReadRange.getNumberOfMessages(), timeout, messageConfig);

        return batchOfBytes.map(bytes -> Arrays.copyOfRange(bytes, topicReadRange.getStartOffset().getOffsetFromStartMessage(),
                bytes.length - topicReadRange.getEndOffset().getMessageOffset())).orElseGet(() -> new byte[0]);
    }

    @SneakyThrows
    public MessageConfig getLatestConfig() {
        ConsumerRecord<Bytes, Bytes> record = topicReader.readPartition(new TopicPartition("/fuse_config:message_config", 0),
                0, 1000L, 200L).reduce((first, second) -> second).orElse(null);
        ObjectMapper mapper = new ObjectMapper();
        if (Objects.isNull(record)) {
            return new MessageConfig();
        }
        String message = record.value().toString();
        return mapper.readValue(message, MessageConfig.class);
    }

    private void updatePartitionSize(final TopicPartition partition) {
        if (!topicSizeData.containsKey(partition)) {
            topicSizeData.put(partition, new LinkedList<>());
        }
        topicReader.readPartition(partition, topicSizeData.get(partition).size(), topicSizeData.get(partition).size(), 200L)
                .forEach(record -> topicSizeData.get(partition)
                        .addLast(record.value().get().length + getSeparatorsLength()));
    }

    private void updateMessageConfig() {
        messageConfig = getLatestConfig();
    }

    private Integer getSeparatorsLength() {
        return (messageConfig.getSeparator() + messageConfig.getStart() + messageConfig.getStop()).getBytes().length;
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
