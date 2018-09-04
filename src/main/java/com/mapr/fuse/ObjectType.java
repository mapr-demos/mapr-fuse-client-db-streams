package com.mapr.fuse;

public enum ObjectType {
    // a MapR stream, topic or partition
    STREAM, TOPIC, PARTITION,
    // a MapR table (not supported yet, but should be detected)
    TABLE,
    // conventional file system objects
    DIRECTORY, FILE, LINK
}
