package com.mapr.fuse.utils;

import java.nio.file.Path;

public class ConvertUtils {
    private final static String TOPIC_NAME_PATTERN = "%s:%s";

    public static String getTopicName(Path path) {
        return path.getFileName().toString();
    }

    public static String transformToTopicName(Path stream, String topic) {
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
