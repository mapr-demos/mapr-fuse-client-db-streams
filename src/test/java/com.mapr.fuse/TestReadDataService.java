package com.mapr.fuse;

import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.dto.MessageConfig;
import com.mapr.fuse.service.ReadDataService;
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
    public final static int MESSAGE_AMOUNT = 7;

    @Before
    public void init() throws Exception {
        messageConfig = new MessageConfig("START", "END", "|", false);

        byte[] configArray = new ObjectMapper().writeValueAsBytes(messageConfig);
        Stream<Bytes> configStream = Stream.of(new Bytes(configArray));

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition("/fuse_config:message_config", 0)),
                anyLong(), anyLong())).thenReturn(configStream);
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
        String msg = getRandomString(MIN_MESSAGE_LENGTH, MAX_MESSAGE_LENGTH);
        String topic = "test_topic";
        int partition = 1;

        int separatorsLength = getSeparatorsLength();

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), anyLong())).thenReturn(Stream.of(new Bytes(msg.getBytes())));

        Assert.assertEquals(Integer.valueOf(msg.length() + separatorsLength), readDataService.requestTopicSizeData(topic, partition));
    }

    @Test
    public void testReadRequiredBytesFromTopicPartition() {
        String topic = "test_topic";
        int partition = 1;
        List<String> messageList = getRandomMessages(MESSAGE_AMOUNT, MIN_MESSAGE_LENGTH, MAX_MESSAGE_LENGTH);

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), anyLong())).thenReturn(Stream.of(convertMessagesToBytes(messageList)));

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), eq(1000L))).thenReturn(Stream.of(convertMessagesToBytes(messageList)));

        readDataService.requestTopicSizeData(topic, partition);

        String resultMessage = messageList.stream().map(msg -> formatMessage(messageConfig, msg))
                .collect(Collectors.joining());

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(topic, partition), 0L, 1000L, 1000L));

        Assert.assertEquals(resultMessage, msgFromService);
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
        int targetStringLength = ThreadLocalRandom.current().nextInt(minLength, maxLength + 1);

        Random random = new Random();

        StringBuilder buffer = new StringBuilder(targetStringLength);
        for (int i = 0; i < targetStringLength; i++) {
            int randomLimitedInt = leftLimit + (int)
                    (random.nextFloat() * (rightLimit - leftLimit + 1));
            buffer.append((char) randomLimitedInt);
        }

       return buffer.toString();
    }

    public int getSeparatorsLength() {
        return (messageConfig.getSeparator() + messageConfig.getStart() + messageConfig.getStop()).getBytes().length;
    }

    private String formatMessage(MessageConfig messageConfig, String message) {
        return String.format("%s%s%s%s",
                messageConfig.getStart(), message, messageConfig.getStop(), messageConfig.getSeparator());
    }
}
