package com.mapr.fuse.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MessageRange {
    private Integer topicOffset;
    private Integer messageOffset;
    private Integer messageSize;
}
