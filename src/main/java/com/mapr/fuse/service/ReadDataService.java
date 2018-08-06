package com.mapr.fuse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.entity.MessageRange;
import com.mapr.fuse.entity.TopicRange;
import com.mapr.fuse.utils.MessageUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
                .map(x -> x + MessageUtils.getSeparatorsLength(messageConfig) +
                        (messageConfig.getSize() ? String.valueOf(x).length() : 0))
                .sum();
    }

    public byte[] readRequiredBytesFromTopicPartition(final TopicPartition partition, final Long startOffset,
            final Long numberOfBytes, final Long timeout) {
        TopicRange topicReadRange =
                calculateReadRange(partition, startOffset, numberOfBytes);

        Optional<byte[]> batchOfBytes = readAndFormat(partition, topicReadRange, timeout);

        return batchOfBytes.map(bytes -> Arrays.copyOfRange(bytes, 0,
                numberOfBytes.intValue())).orElseGet(() -> new byte[0]);
    }

    @SneakyThrows
    public MessageConfig getLatestConfig() {
        Bytes record = topicReader.readPartition(new TopicPartition("/fuse_config:message_config", 0),
                0, 200L).reduce((first, second) -> second).orElse(null);
        ObjectMapper mapper = new ObjectMapper();
        if (Objects.isNull(record)) {
            return new MessageConfig();
        }
        String message = record.toString();
        return mapper.readValue(message, MessageConfig.class);
    }

    private void updatePartitionSize(final TopicPartition partition) {
        if (!topicSizeData.containsKey(partition)) {
            topicSizeData.put(partition, new LinkedList<>());
        }
        topicReader.readPartition(partition, topicSizeData.get(partition).size(), topicSizeData.get(partition).size(), 200L)
                .forEach(record -> topicSizeData.get(partition)
                        .addLast(record.get().length));
    }

    private void updateMessageConfig() {
        messageConfig = getLatestConfig();
    }

    private Optional<byte[]> readAndFormat(final TopicPartition partition, final TopicRange topicRange, final long timeout) {
        AtomicInteger index = new AtomicInteger();

        return topicReader.readPartition(partition, topicRange.getStartOffset().getTopicOffset(),
                topicRange.getNumberOfMessages(), timeout).limit(topicRange.getNumberOfMessages())
                .map(record -> index.getAndIncrement() == 0 ?
                     MessageUtils.formatMessage(messageConfig,
                        new String(record.get()).substring(topicRange.getStartOffset().getOffsetFromStartMessage()),
                        topicRange.getStartOffset().getOffsetFromStartMessage() != 0).getBytes():
                     MessageUtils.formatMessage(messageConfig, new String(record.get()), false).getBytes())
                .reduce(ArrayUtils::addAll);
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
