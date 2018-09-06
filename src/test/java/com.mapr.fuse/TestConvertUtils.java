package com.mapr.fuse;

import com.mapr.fuse.utils.ConvertUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class TestConvertUtils {

    @Test
    public void getStreamNameTest() {
        Path root =  new File("/folder1/folder2").toPath();
        Path stream = new File("/folder1/folder2/folder3/stream").toPath();
        String expected = "/folder3/stream";

        String streamName = ConvertUtils.getStreamName(root, stream);

        Assert.assertEquals(expected, streamName);
    }

    @Test
    public void getTopicNameTest() {
        Path path = new File("/folder/stream/topic").toPath();
        String expected = "topic";

        String topicName = ConvertUtils.getTopicName(path);

        Assert.assertEquals(expected, topicName);
    }

    @Test
    public void transformToTopicNameTest() {
        String stream = "/stream";
        String topic = "topic";
        String expected = "/stream:topic";

        String topicName = ConvertUtils.transformToTopicName(stream, topic);

        Assert.assertEquals(expected, topicName);
    }

    @Test
    public void getPartitionIdTest() {
        Path path = new File("/folder/stream/topic/21").toPath();
        int expected = 21;

        int partitionId = ConvertUtils.getPartitionId(path);

        Assert.assertEquals(expected, partitionId);
    }

    @Test
    public void getFullPathTest() {
        Path root =  new File("/folder1/folder2").toPath();
        String path = "/folder3/stream/topic/0";
        String expected = "/folder1/folder2/folder3/stream/topic/0";

        Path fullPath = ConvertUtils.getFullPath(root, path);

        Assert.assertEquals(expected, fullPath.toString());
    }

}
