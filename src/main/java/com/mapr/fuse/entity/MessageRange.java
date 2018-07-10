package com.mapr.fuse.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MessageRange {
    private Integer topicOffset;
    private Integer messageOffset;
    private Integer messageSize;

    /**
     * @return offset from the start of message
     */
    public Integer getOffsetFromStartMessage() {
        int offsetFromStart = 0;
        if (messageOffset != 0) {
            offsetFromStart = messageSize - messageOffset;
        }
        return offsetFromStart;
    }
}
