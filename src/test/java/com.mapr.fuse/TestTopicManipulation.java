package com.mapr.fuse;

import com.mapr.streams.Admin;
import com.mapr.streams.Streams;

import java.io.IOException;

/**
 * Do some simple stream manipulations to make sure we understand the library
 */
public class TestTopicManipulation {
    public static void main(String[] args) throws IOException {
        Admin admin = Streams.newAdmin(null);
        admin.createStream("/foo", Streams.newStreamDescriptor());
        admin.createTopic("/foo", "t1");
        admin.createTopic("/foo", "t2", 3);

        System.out.printf("%d topics\n", admin.countTopics("/foo"));
        admin.listTopics("/foo").forEach(
                x -> System.out.printf("   %s\n", x)
        );
    }
}
