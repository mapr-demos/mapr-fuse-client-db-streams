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

import java.util.Optional;
import java.util.stream.Stream;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ReadDataService.class)
public class TestReadDataService {
    @Mock
    public TopicReader topicReader;

    @InjectMocks
    public ReadDataService readDataService;

    @Before
    public void init() throws Exception {
        byte[] config = new ObjectMapper().writeValueAsBytes(new MessageConfig());
        Stream<Bytes> configStream = Stream.of(new Bytes(config));

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition("/fuse_config:message_config", 0)),
                anyLong(), anyLong())).thenReturn(configStream);
    }


    @Test
    public void testGetLatestConfig() {
        MessageConfig messageConfigFromService = readDataService.getLatestConfig();

        MessageConfig correctMessageConfig = new MessageConfig();
        Assert.assertEquals(correctMessageConfig, messageConfigFromService);

        MessageConfig wrongMessageConfig = new MessageConfig("", "", "", true);
        Assert.assertNotEquals(wrongMessageConfig, messageConfigFromService);
    }

    @Test
    public void testRequestTopicSizeData() {
        String msg = "test";
        String topic = "test_topic";
        int partition = 1;

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), anyLong())).thenReturn(Stream.of(new Bytes(msg.getBytes())));

        Assert.assertEquals(Integer.valueOf(msg.length()), readDataService.requestTopicSizeData(topic, partition));
    }


    @Test
    public void testReadRequiredBytesFromTopicPartition() {
        String msg = "test";
        String topic = "test_topic";
        int partition = 1;

        when(topicReader.readPartition(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), anyLong())).thenReturn(Stream.of(new Bytes(msg.getBytes())));

        readDataService.requestTopicSizeData(topic, partition);

        when(topicReader.readAndFormat(ArgumentMatchers.eq(new TopicPartition(topic, partition)),
                anyLong(), anyLong(), anyLong(), ArgumentMatchers.any())).thenReturn(Optional.of(msg.getBytes()));

        String msgFromService = new String(readDataService.readRequiredBytesFromTopicPartition(
                new TopicPartition(topic, partition), 0L, 1000L, 200L));

        Assert.assertEquals(msg, msgFromService);
    }
}
