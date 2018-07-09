package com.mapr.fuse.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicRange {
    private MessageRange startOffset;
    private MessageRange endOffset;

    public Integer getNumberOfMessages() {
        return endOffset.getTopicOffset() - startOffset.getTopicOffset();
    }
}
