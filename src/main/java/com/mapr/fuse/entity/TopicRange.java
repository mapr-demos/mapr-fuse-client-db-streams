package com.mapr.fuse.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class TopicRange {
    private MessageRange startOffset;
    private MessageRange endOffset;

    /**
     * @return number of messages in the range
     */
    public Integer getNumberOfMessages() {
        int numberOfMessages = endOffset.getTopicOffset() - startOffset.getTopicOffset();
        if (endOffset.getMessageOffset() > 0) {
            numberOfMessages++;
        }
        return numberOfMessages;
    }
}
