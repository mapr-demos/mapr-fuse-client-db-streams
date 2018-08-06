package com.mapr.fuse;

import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.service.ReadDataService;
import com.mapr.fuse.utils.MessageUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(value = PowerMockRunner.class)
@PrepareForTest(ReadDataService.class)
public class TestReadDataService {
    @Mock
    public TopicReader topicReader;

    @InjectMocks
    public ReadDataService readDataService;

    public MessageConfig messageConfig;

    public final static int MIN_MESSAGE_LENGTH = 5;
    public final static int MAX_MESSAGE_LENGTH = 20;
    public final static int MESSAGE_AMOUNT = 10;
    public final static int START_CUTOFF = 3;
    public final static int MESSAGE_LENGTH = 30;

    public final static String TOPIC_NAME = "test_topic";
    public final static int PARTITION = 1;
    public List<String> messageList;

    @Before
    public void init() throws Exception {
        messageConfig = new MessageConfig("START", "END", "|", true);
        messageList = getRandomMessages(MESSAGE_AMOUNT, MIN_MESSAGE_LENGTH, MAX_MESSAGE_LENGTH);

        byte[] configArray = new ObjectMapper().writeValueAsBytes(messageConfig);
        Stream<Bytes> configStream = Stream.of(new Bytes(configArray));

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition("/fuse_config:message_config", 0)),
                anyLong(), anyLong())).thenReturn(configStream);

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(TOPIC_NAME, PARTITION)),
                anyLong(), anyLong(), anyLong())).thenReturn(Stream.of(convertMessagesToBytes(messageList)));

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(TOPIC_NAME, PARTITION)),
                anyLong(), anyLong(), eq(1000L))).thenReturn(Stream.of(convertMessagesToBytes(messageList)));
    }

    @Test
    public void testGetLatestConfig() {
        MessageConfig messageConfigFromService = readDataService.getLatestConfig();

        Assert.assertEquals(messageConfig, messageConfigFromService);

        MessageConfig wrongMessageConfig = new MessageConfig("", "", "", true);
        Assert.assertNotEquals(wrongMessageConfig, messageConfigFromService);
    }

    @Test
    public void testRequestTopicSizeData() {
        String resultMessage = messageList.stream().map(msg ->
                MessageUtils.formatMessage(messageConfig, msg, false))
                .collect(Collectors.joining());

        Assert.assertEquals(Integer.valueOf(resultMessage.getBytes().length),
                readDataService.requestTopicSizeData(TOPIC_NAME, PARTITION));
    }

    @Test
    public void testReadRequiredBytesFromTopicPartition() {
        readDataService.requestTopicSizeData(TOPIC_NAME, PARTITION);

        String resultMessage = messageList.stream().map(msg ->
                MessageUtils.formatMessage(messageConfig, msg, false))
                .collect(Collectors.joining());

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(TOPIC_NAME, PARTITION), 0L, (long) resultMessage.length(), 1000L));

        Assert.assertEquals(resultMessage, msgFromService);
    }

    @Test
    public void testReadRequiredBytesFromTopicPartitionWithStartCutoff() {
        readDataService.requestTopicSizeData(TOPIC_NAME, PARTITION);

        AtomicInteger index = new AtomicInteger();

        String resultMessage = messageList.stream().map(msg ->
                index.getAndIncrement() == 0 ?
                    MessageUtils.formatMessage(messageConfig, msg.substring(START_CUTOFF), true) :
                    MessageUtils.formatMessage(messageConfig, msg, false))
                .collect(Collectors.joining());

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(TOPIC_NAME, PARTITION), (long) START_CUTOFF, (long) resultMessage.length() , 1000L));

        Assert.assertEquals(resultMessage, msgFromService);
    }

    @Test
    public void testReadRequiredBytesFromTopicPartitionWithEndCutoff() {
        readDataService.requestTopicSizeData(TOPIC_NAME, PARTITION);

        String resultMessage = messageList.stream().map(msg ->
                MessageUtils.formatMessage(messageConfig, msg, false))
                .collect(Collectors.joining());

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(TOPIC_NAME, PARTITION), 0L, (long) MESSAGE_LENGTH, 1000L));

        Assert.assertEquals(new String(Arrays.copyOfRange(resultMessage.getBytes(), 0 , MESSAGE_LENGTH)), msgFromService);
    }

    @Test
    public void testReadRequiredBytesFromTopicPartitionWithStartAndEndCutoff() {
        readDataService.requestTopicSizeData(TOPIC_NAME, PARTITION);

        AtomicInteger index = new AtomicInteger();

        String resultMessage = messageList.stream().map(msg ->
                index.getAndIncrement() == 0 ?
                        MessageUtils.formatMessage(messageConfig, msg.substring(START_CUTOFF), true) :
                        MessageUtils.formatMessage(messageConfig, msg, false))
                .collect(Collectors.joining());

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(TOPIC_NAME, PARTITION), (long) START_CUTOFF, (long) MESSAGE_LENGTH, 1000L));

        Assert.assertEquals(new String(Arrays.copyOfRange(resultMessage.getBytes(), 0 , MESSAGE_LENGTH)), msgFromService);
    }

    public Bytes[] convertMessagesToBytes(List<String> messageList) {
        return messageList.stream().map(m -> new Bytes(m.getBytes())).toArray(Bytes[]::new);
    }

    public List<String> getRandomMessages(int amount, int minLength, int maxLength) {
        List<String> msgList = new LinkedList<>();

        for(int i = 0; i < amount; i++)
            msgList.add(getRandomString(minLength, maxLength));

        return msgList;
    }

    public String getRandomString(int minLength, int maxLength) {
        int leftLimit = 48;
        int rightLimit = 122;
        int targetStringLength = ThreadLocalRandom.current().nextInt(minLength, maxLength - 1);

        Random random = new Random();

        StringBuilder buffer = new StringBuilder(targetStringLength);
        buffer.append("{");
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }
        buffer.append("}");

       return buffer.toString();
    }
}
