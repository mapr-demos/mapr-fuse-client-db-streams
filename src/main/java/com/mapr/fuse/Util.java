package com.mapr.fuse;

public class Util {
    public static String getFullPath(String root, String partial) {
        if (partial.startsWith("/")) {
            partial = partial.substring(1);
        }
        return root + partial;
    }
}
