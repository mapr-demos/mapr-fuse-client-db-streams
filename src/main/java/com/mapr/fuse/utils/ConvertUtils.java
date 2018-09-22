package com.mapr.fuse.utils;

import lombok.experimental.UtilityClass;

import java.nio.file.Path;

@UtilityClass
public class ConvertUtils {
    private final static String TOPIC_NAME_PATTERN = "%s:%s";

    public static String getStreamName(Path root, Path path) {
        return path.toString().replace(root.toString(), "");
    }

    public static String getTopicName(Path path) {
        return path.getFileName().toString();
    }

    public static String transformToTopicName(String stream, String topic) {
        return String.format(TOPIC_NAME_PATTERN, stream, topic);
    }

    public static int getPartitionId(Path path) {
        return Integer.parseInt(path.getFileName().toString());
    }

    public static Path getFullPath(Path root, String partial) {
        if (partial.startsWith("/")) {
            partial = partial.substring(1);
        }
        return root.resolve(partial);
    }
}
