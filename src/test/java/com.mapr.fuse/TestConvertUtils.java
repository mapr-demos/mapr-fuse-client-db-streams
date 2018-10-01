package com.mapr.fuse;

import com.mapr.fuse.utils.ConvertUtils;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class TestConvertUtils {
    @Test
    public void getTopicNameTest() {
        Path path = new File("/folder/stream/topic").toPath();
        assertEquals("topic", ConvertUtils.getTopicName(path));
    }

    @Test
    public void transformToTopicNameTest() {
        Path stream = new File("/stream").toPath();
        String topicName = ConvertUtils.transformToTopicName(stream, "topic");
        assertEquals("/stream:topic", topicName);
    }

    @Test
    public void getPartitionIdTest() {
        Path path = new File("/folder/stream/topic/21").toPath();
        assertEquals(21, ConvertUtils.getPartitionId(path));
    }

    @Test
    public void getFullPathTest() {
        Path root = new File("/mapr/cluster").toPath();
        Path fullPath = ConvertUtils.getFullPath(root, "/folder3/stream/topic/0");

        assertEquals("/mapr/cluster/folder3/stream/topic/0", fullPath.toString());
    }

}
