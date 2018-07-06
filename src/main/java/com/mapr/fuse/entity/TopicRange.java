package com.mapr.fuse.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicRange {
    private Integer startOffset;
    private Integer endOffset;

    public Integer getNumberOfMessages() {
        return endOffset - startOffset;
    }
}
